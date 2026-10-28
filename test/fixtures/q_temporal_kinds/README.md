# q temporal kinds round-trip fixture

Context: RayforceDB/rayforce issue 1053. Defines the expected q kind for atoms, vectors and one-column tables of the q temporal types, including nulls and range cases.

Regenerate: `QHOME=<q home> q fixture.q -q > fixture.out` (kdb+ 4.0, deterministic, no network).

Columns of `fixture.out` (pipe separated):

- `kind`: q type letter of the value (`z` datetime, `m` month, `u` minute, `v` second; `p`, `d`, `t` are controls that already round-trip).
- `form`: `atom`, `vector` (normal values plus a typed null) or `table` (one column `x` holding that vector).
- `case`: which value is encoded.
- `outcome`: `ok` round-trips exactly at the supported precision; `null` must come back as the typed null of the kind; `inf` is a q infinity, which may map to null or to a typed infinity but the choice must be documented.
- `qtype`: q type code of the value (negative atom, positive vector, 98 table).
- `expected_parity`: kind a q client must read back for lossless kind round-trip.
- `expected_today`: kind read back with rayforce-q 3267409 (datetime as `p`, month as `d`, minute/second as `t`).
- `hex`: `-8!` frame of the value, little-endian, exactly as q emits it.

Lossless here means round-trip of the q kind for supported values (datetime at millisecond precision within the TIMESTAMP range, month as whole months, typed nulls), not bit preservation of arbitrary payloads.
