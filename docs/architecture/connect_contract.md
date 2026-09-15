# How to connect — address, tokens and grants

The other half of the status API. [status_api.md](status_api.md) says *what each route
answers*; this says *how to reach it and what a token has to hold*. A contract that
describes the answers but not the connection is half a contract, and the half that is
missing is the one that fails at three in the morning.

Not in here: what the routes return. Not in here either: how to rotate a credential on a
public edge — there is no public edge yet, and a procedure written before the thing it
governs is a procedure nobody follows.

---

## The address

There is no public hostname today. The collector binds the loopback interface and nothing
fronts it yet.

```
http://127.0.0.1:8110              from the machine the collector runs on
http://host.docker.internal:8110   from a container on that machine
```

**When a public edge is added, configure the hostname and never the address behind it.** A
hostname makes a later move a DNS change on our side and nothing on yours; an IP makes the
same move a coordinated break across two projects. TLS will terminate in a reverse proxy
that forwards to the loopback port, the way the sister projects do it. The port itself
gets no firewall rule and does not get one later.

### Binding, and the one place it differs

`api.host` defaults to `127.0.0.1`, which is correct when the collector runs in a
virtualenv on the server.

**Inside the dev container it has to be `0.0.0.0`.** Containment there comes from the
compose publish — `127.0.0.1:8110:8110` — not from the bind: a process bound to loopback
*inside* a container is unreachable through a published port. Setting `0.0.0.0` on the
server instead would expose the port to the network, which is the mistake this paragraph
exists to prevent. The value belongs in the machine's `user_configs/` overlay, not in the
tracked default.

---

## The scheme

A bearer token in the `Authorization` header:

```
Authorization: Bearer <token>
```

Three routes need none — `/v1/health`, `/v1/build` and `/openapi.json`. Every other route needs a token
**and** a grant naming the surface it touches.

```
401   no token, or a token this collector does not carry
403   a valid token that does not hold the surface
```

The distinction is deliberate. A 403 tells you the credential is fine and the permission
is not, which is a denial you can act on rather than one that sends you hunting for a bad
token. The refusal names what the token *does* hold.

## Grants

A grant is `<surface>:<name>`. The surfaces this collector declares:

| Surface | Route | What it exposes |
|---|---|---|
| `status` | `/v1/status` | symbol names, tick counts, buffers, reconnects, disk |
| `config` | `/v1/configs` | the effective configuration, credentials removed |
| `archive` | `/v1/archive` | the file inventory |
| `logs` | `/v1/logs` | log excerpts |
| `files` | `/v1/files/{name}` | a finished archive file, handed out |

**The surface vocabulary is closed and checked when the configuration is parsed.** A grant
naming a surface that does not exist — `statsu:detail` — is refused at boot rather than
becoming a denial at request time nobody can explain. The *name* after the colon is not
checked: `status:detial` parses and then denies. So a surface typo cannot reach production
and a name typo can, reading as a permission problem.

**Filters are query parameters, identities are path parameters**, and the difference
decides the grant. `finiex_auth` derives the grant name from the path parameter, so a date
or a symbol there would demand a grant per calendar day or per instrument — those are
query parameters, and the surface itself is the permission. A file name *is* an identity,
so `/v1/files/{name}` gates on `files:<name>` and a consumer entitled to the archive holds
`files:*`.

## Issuing a token

In `user_configs/app_config.json`, which is gitignored. The tracked
`configs/app_config.json` carries an empty token set, and an empty registry refuses every
gated route — access is granted by writing a name down, never by a default nobody chose.

```json
"api": {
  "enabled": true,
  "host": "127.0.0.1",
  "port": 8110,
  "tokens": {
    "testingide": {
      "token": "<generate one>",
      "grants": ["status:detail", "archive:index"],
      "note": "liveness and archive inventory"
    }
  }
}
```

```bash
python -c "import secrets; print(secrets.token_urlsafe(32))"
```

**Never use the bare `name: token` string form** that `finiex_auth` also accepts. It grants
`*` — every surface, including ones added after the token was issued. One grant of `*` is
indistinguishable from a deliberate decision six months later.

**A secret never travels over the cross-project bus.** That folder is readable by every
peer; a credential there is a credential shared with everyone. Tokens are handed over out
of band, and the bus carries only the fact that one exists and what it holds.

## Alternative: the environment

```
FINIEX_COLLECTOR_TOKENS=name:token,other:token
```

Takes precedence over the configuration when set. Tokens supplied this way receive
`status:detail` and nothing else — the environment carries no grant vocabulary, and
inventing a wider default here is how `*` gets in through a side door. For anything beyond
liveness, use the configuration form.

The variable is named for this collector on purpose. A shared name would let one service's
token authenticate against another.

---

## What the package provides, and what stays here

`finiex_auth` is the shared implementation, pinned by tag in `requirements.txt` and the
same one FiniexTestingIDE and FiniexRAGEngine use. It provides the bearer and grant
dependencies, the token registry, the credential vocabulary used for redaction, and a
route walk for suites.

**It ships no routes.** Every route this collector serves is its own, including `/v1/health`
and `/v1/build`.

What stays project-specific is deliberately small: where the credentials live, which
environment variable names them, and the surface vocabulary. The package never learns any
of that — a shared variable name or a shared surface list would couple two services that
should only share a mechanism.
