//go:build integration

package integration

import (
	"context"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/spf13/viper"
	"github.com/tjbdwanghaibo/roost-core/app"
	fredis "github.com/tjbdwanghaibo/roost-core/redis"
	"github.com/tjbdwanghaibo/roost-core/security"
	kitmods "github.com/tjbdwanghaibo/roost-kit/mods"

	"github.com/tjbdwanghaibo/roost-service/account"
	"github.com/tjbdwanghaibo/roost-service/chat"
	"github.com/tjbdwanghaibo/roost-service/directory"
	"github.com/tjbdwanghaibo/roost-service/global"
	"github.com/tjbdwanghaibo/roost-service/mail"
	"github.com/tjbdwanghaibo/roost-service/match"
	"github.com/tjbdwanghaibo/roost-service/platform"
	"github.com/tjbdwanghaibo/roost-service/rank"
	"github.com/tjbdwanghaibo/roost-service/servicemods"
	"github.com/tjbdwanghaibo/roost-service/session"
)

// This is the end-to-end proof for the deployment surface: every service Mod
// is taken through the lifecycle an app puts it through — Init from
// configuration, Provide against a registry holding a real Redis — and every
// capability it claims to publish is then looked up and USED.
//
// Driving the lifecycle by hand rather than through app.App is deliberate.
// App.run is private and Execute runs a whole cobra command, so a test that
// went through it would be testing App's plumbing, which roost-core already
// tests. What is untested until here is the Mods' own contract: that Init
// accepts a realistic configuration, that Provide finds what it needs, and
// that what lands in the registry is the type a caller will assert on.

// modConfig is one configuration for all nine services, shaped the way a
// deployment's roost.yaml would be — including the required values that have
// no defaults, since a config missing any of them is supposed to fail.
func modConfig(root string) *viper.Viper {
	cfg := viper.New()
	for service, prefix := range map[string]string{
		"account": ":account", "chat": ":chat", "directory": ":directory",
		"global": ":global", "mail": ":mail", "match": ":match",
		"platform": ":platform", "rank": ":rank", "session": ":session",
	} {
		cfg.Set(service+".key_prefix", root+prefix)
	}
	cfg.Set("account.session_secret", "itest-account-secret")
	cfg.Set("directory.reservation_ttl", time.Minute)
	cfg.Set("global.reservation_ttl", 30*time.Minute)
	cfg.Set("mail.send_ttl", 24*time.Hour)
	cfg.Set("platform.session_secret", "itest-platform-session")
	cfg.Set("platform.payment_secret", "itest-platform-payment")
	cfg.Set("session.request_ttl", time.Hour)
	return cfg
}

// serviceMods builds every Mod with the policy collaborators a business repo
// would supply. Every one of these is required with no default, which is the
// point: this list is what a deployment must decide, written out.
func serviceMods(t *testing.T) []app.Mod {
	t.Helper()
	var nextPlayer int64 = 7000
	registry := chat.NewBodyRegistry()
	if err := registry.Register("text", chat.BodySpec{Validator: chat.TextValidator(200)}); err != nil {
		t.Fatal(err)
	}
	return []app.Mod{
		account.NewMod(
			account.VerifierFunc(func(_ context.Context, id account.Identity) (account.Verified, error) {
				return account.Verified{Channel: id.Channel, OpenID: id.OpenID}, nil
			}),
			account.AllocatorFunc(func(context.Context, int32) (int64, error) {
				nextPlayer++
				return nextPlayer, nil
			}),
			account.NameValidatorFunc(func(string) error { return nil }),
			nil,
		),
		chat.NewMod(permissivePolicy{}, registry,
			chat.SystemAuthenticatorFunc(func(context.Context) (chat.SystemToken, error) {
				return chat.GrantSystem(), nil
			}), nil, nil),
		directory.NewMod(directory.NormalizeLower, nil),
		global.NewMod(nil),
		mail.NewMod(nil, nil),
		match.NewMod(nil, nil),
		platform.NewMod(
			platform.VerifierFunc(func(_ context.Context, c platform.Credential) (platform.Verified, error) {
				return platform.Verified{Channel: c.Channel, OpenID: c.OpenID}, nil
			}),
			platform.PlayerResolverFunc(func(context.Context, platform.Verified) (int64, error) { return 1001, nil }),
			platform.DelivererFunc(func(context.Context, platform.Order) error { return nil }),
			nil,
		),
		rank.NewMod(nil),
		session.NewMod(
			session.ReleaserFunc(func(context.Context, session.Run, session.Resource) error { return nil }),
			nil,
		),
	}
}

// bootstrap takes every Mod through Init and Provide against a registry
// holding a live Redis, the way an app does.
func bootstrap(t *testing.T) (*app.Registry, *viper.Viper) {
	t.Helper()
	c := client(t)
	cfg := modConfig(prefix(t, "mods"))
	registry := app.NewRegistry(cfg)
	// Stand in for roost-kit's RedisMod, which is what publishes this
	// capability in a real process.
	if err := registry.Register(kitmods.ModRedis, c); err != nil {
		t.Fatal(err)
	}
	for _, mod := range serviceMods(t) {
		if err := mod.Init(cfg); err != nil {
			t.Fatalf("%s Init: %v", mod.Name(), err)
		}
		if err := mod.Provide(registry); err != nil {
			t.Fatalf("%s Provide: %v", mod.Name(), err)
		}
		if err := mod.Start(); err != nil {
			t.Fatalf("%s Start: %v", mod.Name(), err)
		}
		t.Cleanup(mod.Stop)
	}
	return registry, cfg
}

// Every capability in the name table is published by some Mod, and none is
// missing. A name in the table that nothing registers is a name a business
// repo will look up and not find.
func TestEveryDeclaredCapabilityIsPublished(t *testing.T) {
	registry, _ := bootstrap(t)
	for _, name := range servicemods.All {
		if _, ok := registry.Get(name); !ok {
			t.Fatalf("capability %q is in the name table but no Mod published it", name)
		}
	}
}

// Each capability resolves to the type a caller will assert on, and the value
// WORKS — not merely that something non-nil is present. A registry holding a
// service that cannot reach its store is a process that starts and then fails
// on the first request.
func TestEveryPublishedCapabilityIsUsable(t *testing.T) {
	registry, _ := bootstrap(t)
	ctx := context.Background()

	t.Run("account", func(t *testing.T) {
		service, ok := app.Lookup[*account.Service](registry, servicemods.ModAccount)
		if !ok {
			t.Fatal("the account capability is not an *account.Service")
		}
		if _, err := service.UpsertServer(ctx, account.Server{ID: 1, Name: "s1", Status: account.ServerOpen}); err != nil {
			t.Fatal(err)
		}
		acct, err := service.Login(ctx, account.Identity{Channel: "store", OpenID: "u1", Credential: "good"})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := service.CreateRole(ctx, acct.ID, 1, "Alice"); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("chat", func(t *testing.T) {
		service, ok := app.Lookup[*chat.Service](registry, servicemods.ModChat)
		if !ok {
			t.Fatal("the chat capability is not a *chat.Service")
		}
		if _, err := service.Publish(ctx, chat.Sender{RoleID: 1, Name: "role-1"}, chat.PublishRequest{
			Channel: chat.Channel{Kind: chat.ChannelWorld, Target: 7},
			Type:    "text", Body: []byte("hello"), RequestID: "r1",
		}); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("directory", func(t *testing.T) {
		dir, ok := app.Lookup[directory.Directory](registry, servicemods.ModDirectory)
		if !ok {
			t.Fatal("the directory capability is not a directory.Directory")
		}
		if _, err := dir.Reserve(ctx, "Zed", "owner-1", 0); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("global", func(t *testing.T) {
		service, ok := app.Lookup[*global.Service](registry, servicemods.ModGlobal)
		if !ok {
			t.Fatal("the global capability is not a *global.Service")
		}
		if _, err := service.Bind(ctx, 7, "group-a", 100); err != nil {
			t.Fatal(err)
		}
		if _, err := service.AcquireLease(ctx, 7); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("global activity", func(t *testing.T) {
		activity, ok := app.Lookup[*global.ActivityService](registry, servicemods.ModGlobalActivity)
		if !ok {
			t.Fatal("the global activity capability is not a *global.ActivityService")
		}
		key := global.ActivityKey{GroupID: "group-a", ActivityID: "act-1", Phase: "settle"}
		if _, err := activity.OpenActivity(ctx, key, []int32{7}); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("mail", func(t *testing.T) {
		service, ok := app.Lookup[*mail.Service](registry, servicemods.ModMail)
		if !ok {
			t.Fatal("the mail capability is not a *mail.Service")
		}
		if _, err := service.Send(ctx, mail.SendRequest{
			Audience: mail.AudienceDirect, Recipients: []int64{7}, Subject: "reward",
			ExpiresIn: time.Hour, RequestID: "r1",
		}); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("match", func(t *testing.T) {
		store, ok := app.Lookup[match.Store](registry, servicemods.ModMatch)
		if !ok {
			t.Fatal("the match capability is not a match.Store")
		}
		if _, err := store.Enqueue(ctx,
			match.Queue{Mode: "ranked", GroupSize: 2, Partition: "eu"},
			match.Subject{Kind: "player", ID: 1, Score: 100}, "r1"); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("platform", func(t *testing.T) {
		service, ok := app.Lookup[*platform.Service](registry, servicemods.ModPlatform)
		if !ok {
			t.Fatal("the platform capability is not a *platform.Service")
		}
		issued, err := service.AuthSession(ctx, platform.Credential{
			Channel: "store", OpenID: "u1", Secret: "anything",
		})
		if err != nil {
			t.Fatal(err)
		}
		if err := service.ValidateSession(issued.PlayerID, issued.Token); err != nil {
			t.Fatalf("a token minted through the Mod does not validate: %v", err)
		}
		// And it verifies against the secret CONFIGURATION named, checked
		// independently of the service.
		//
		// Round-tripping through the same service proves nothing about which
		// secret was wired: a Mod that passed the payment secret where the
		// session secret belongs would sign and verify consistently with
		// itself, and only fail against anything else that reads the same
		// config — a gateway, a sibling process. That is a deployment bug that
		// self-verification cannot see, so this asserts the actual value.
		if _, err := security.VerifySessionToken(
			issued.Token, "itest-platform-session", issued.PlayerID, time.Now(),
		); err != nil {
			t.Fatalf("the token does not verify against platform.session_secret from "+
				"configuration: %v; the Mod wired a different secret", err)
		}
	})

	t.Run("rank", func(t *testing.T) {
		store, ok := app.Lookup[rank.Store](registry, servicemods.ModRank)
		if !ok {
			t.Fatal("the rank capability is not a rank.Store")
		}
		board := rank.Board{ID: "arena", Scope: rank.ScopeServer, ScopeID: 1, Season: 1}
		if _, err := store.Submit(ctx, board, rank.Score{OwnerID: 1, Value: 10}, rank.UpdateSet, ""); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("session", func(t *testing.T) {
		service, ok := app.Lookup[*session.Service](registry, servicemods.ModSession)
		if !ok {
			t.Fatal("the session capability is not a *session.Service")
		}
		if _, err := service.Enter(ctx, 1, session.EnterRequest{Kind: "d7", RequestID: "r1"}); err != nil {
			t.Fatal(err)
		}
	})
}

// The Mods' key prefixes come from configuration, so the whole set must land
// under the configured root. A Mod that hardcoded a prefix, or read the wrong
// config key, would write outside it — and two deployments sharing one Redis
// would then share state, which is exactly what the required prefix prevents.
//
// It is measured as a before/after difference over the WHOLE keyspace rather
// than by scanning for a suspected pattern. Scanning for one pattern only
// finds the strays you already guessed at, and it also picks up leftovers
// from earlier runs — which made an earlier version of this test report a
// stray that a previous test had written, turning two mutation checks green
// while looking red.
func TestEveryModWritesUnderItsConfiguredPrefix(t *testing.T) {
	c := client(t)
	root := prefix(t, "confined")
	cfg := modConfig(root)
	registry := app.NewRegistry(cfg)
	if err := registry.Register(kitmods.ModRedis, c); err != nil {
		t.Fatal(err)
	}
	for _, mod := range serviceMods(t) {
		if err := mod.Init(cfg); err != nil {
			t.Fatalf("%s Init: %v", mod.Name(), err)
		}
		if err := mod.Provide(registry); err != nil {
			t.Fatalf("%s Provide: %v", mod.Name(), err)
		}
	}

	before := keySet(t, c)
	driveThroughRegistry(t, registry)
	after := keySet(t, c)

	fresh, stray := 0, []string{}
	for key := range after {
		if before[key] {
			continue
		}
		fresh++
		if !strings.HasPrefix(key, root) {
			stray = append(stray, key)
		}
	}
	if fresh == 0 {
		t.Fatal("no new keys were written; this test would pass vacuously")
	}
	if len(stray) > 0 {
		sort.Strings(stray)
		t.Fatalf("%d of %d new keys were written outside the configured prefix %q, e.g. %q; "+
			"a hardcoded prefix or a misread config key means two deployments sharing one "+
			"redis share state", len(stray), fresh, root, stray[0])
	}

	// And each service writes under its OWN sub-prefix, not merely somewhere
	// under the root.
	//
	// "Under the root" is not enough on its own: a Mod that read another
	// service's config key — session reading rank.key_prefix, say — still
	// lands under the root, so the stray check above passes while two
	// services quietly share a namespace. That mutation came back green until
	// this assertion existed.
	for _, service := range []string{
		"account", "chat", "directory", "global", "mail",
		"match", "platform", "rank", "session",
	} {
		want := root + ":" + service + ":"
		found := false
		for key := range after {
			if !before[key] && strings.HasPrefix(key, want) {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("nothing was written under %q; the %s mod is reading another service's "+
				"config key, so the two share a namespace", want, service)
		}
	}
	t.Logf("%d new keys, all under %q, each service in its own namespace", fresh, root)
}

// keySet snapshots the whole keyspace, so what a test measures is the
// DIFFERENCE it caused rather than whatever matched a guess.
func keySet(t *testing.T, c fredis.IRedis) map[string]bool {
	t.Helper()
	out := map[string]bool{}
	for _, key := range scanKeys(t, c, "*") {
		out[key] = true
	}
	return out
}

// driveThroughRegistry writes at least once through every capability, looked
// up rather than held — so the write really goes through what the Mod
// published.
func driveThroughRegistry(t *testing.T, registry *app.Registry) {
	t.Helper()
	ctx := context.Background()

	acct := app.MustLookup[*account.Service](registry, servicemods.ModAccount)
	if _, err := acct.UpsertServer(ctx, account.Server{ID: 1, Name: "s1", Status: account.ServerOpen}); err != nil {
		t.Fatal(err)
	}
	account1, err := acct.Login(ctx, account.Identity{Channel: "store", OpenID: "u1", Credential: "good"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := acct.CreateRole(ctx, account1.ID, 1, "Alice"); err != nil {
		t.Fatal(err)
	}

	chatSvc := app.MustLookup[*chat.Service](registry, servicemods.ModChat)
	if _, err := chatSvc.Publish(ctx, chat.Sender{RoleID: 1, Name: "role-1"}, chat.PublishRequest{
		Channel: chat.Channel{Kind: chat.ChannelWorld, Target: 7},
		Type:    "text", Body: []byte("hello"), RequestID: "r1",
	}); err != nil {
		t.Fatal(err)
	}

	dir := app.MustLookup[directory.Directory](registry, servicemods.ModDirectory)
	if _, err := dir.Reserve(ctx, "Zed", "owner-1", 0); err != nil {
		t.Fatal(err)
	}

	globalSvc := app.MustLookup[*global.Service](registry, servicemods.ModGlobal)
	if _, err := globalSvc.Bind(ctx, 7, "group-a", 100); err != nil {
		t.Fatal(err)
	}
	if _, err := globalSvc.AcquireLease(ctx, 7); err != nil {
		t.Fatal(err)
	}

	mailSvc := app.MustLookup[*mail.Service](registry, servicemods.ModMail)
	if _, err := mailSvc.Send(ctx, mail.SendRequest{
		Audience: mail.AudienceDirect, Recipients: []int64{7}, Subject: "reward",
		ExpiresIn: time.Hour, RequestID: "r1",
	}); err != nil {
		t.Fatal(err)
	}

	matchStore := app.MustLookup[match.Store](registry, servicemods.ModMatch)
	if _, err := matchStore.Enqueue(ctx,
		match.Queue{Mode: "ranked", GroupSize: 2, Partition: "eu"},
		match.Subject{Kind: "player", ID: 1, Score: 100}, "r1"); err != nil {
		t.Fatal(err)
	}

	platformSvc := app.MustLookup[*platform.Service](registry, servicemods.ModPlatform)
	raw := []byte(`{"order_id":"o1","player_id":1001,"channel":"store",` +
		`"product_id":"gems-100","amount_minor":499,"currency":"USD"}`)
	if _, err := platformSvc.HandleCallback(ctx, raw,
		platform.SignPayload(raw, "itest-platform-payment")); err != nil {
		t.Fatal(err)
	}

	rankStore := app.MustLookup[rank.Store](registry, servicemods.ModRank)
	if _, err := rankStore.Submit(ctx,
		rank.Board{ID: "arena", Scope: rank.ScopeServer, ScopeID: 1, Season: 1},
		rank.Score{OwnerID: 1, Value: 10}, rank.UpdateSet, ""); err != nil {
		t.Fatal(err)
	}

	sessionSvc := app.MustLookup[*session.Service](registry, servicemods.ModSession)
	if _, err := sessionSvc.Enter(ctx, 1, session.EnterRequest{Kind: "d7", RequestID: "r1"}); err != nil {
		t.Fatal(err)
	}
}
