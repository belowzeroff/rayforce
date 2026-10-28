/ q temporal kind round-trip fixture (issue RayforceDB/rayforce#1053). Deterministic, no network.
/ Run: q fixture.q -q
/ Columns: kind|form|case|outcome|qtype|expected_parity|expected_today|hex(-8! frame, LE, as -8! emits)
/ outcome: ok = value round-trips; null = must come back as the typed null of the kind;
/          inf = typed infinity or typed null, engine choice, must be documented.
/ expected_today = q kind a client reads back from rayforce-q 3267409 (z->p, m->d, u/v->t).
hx:{raze string -8!x};
td:`z`m`u`v`p`d`t!`p`d`t`t`p`d`t;
/ each kind: values, outcome labels, case labels (normal values first, then null, then specials)
S:{[k;v;o;c] (k;v;o;c)};
L:();
L,:enlist S[`z;2000.01.01T00:00:00.000,2026.03.14T09:30:15.123,1999.12.31T23:59:59.999,0Nz,0Wz,-0Wz,"z"$1e6;
  `ok`ok`ok`null`inf`inf`null;("epoch";"2026 ms";"1999 neg";"0Nz";"0Wz";"-0Wz";"1e6 days out of range")];
L,:enlist S[`m;1999.12m,2100.06m,0Nm,0Wm,-0Wm;`ok`ok`null`inf`inf;("1999.12";"2100.06";"0Nm";"0Wm";"-0Wm")];
L,:enlist S[`u;00:00u,23:59u,0Nu,0Wu;`ok`ok`null`inf;("00:00";"23:59";"0Nu";"0Wu")];
L,:enlist S[`v;00:00:00v,23:59:59v,0Nv,0Wv;`ok`ok`null`inf;("00:00:00";"23:59:59";"0Nv";"0Wv")];
L,:enlist S[`p;2000.01.01D00:00:00.000000000,2026.03.14D09:30:15.123456789,0Np;`ok`ok`null;("epoch";"2026 ns";"0Np")];
L,:enlist S[`d;2000.01.01,1999.12.31,0Nd;`ok`ok`null;("epoch";"1999 neg";"0Nd")];
L,:enlist S[`t;00:00:00.000,23:59:59.999,0Nt;`ok`ok`null;("00:00";"23:59:59.999";"0Nt")];
emit:{[k;f;c;o;x] -1 "|"sv (string k;f;c;string o;string type x;string k;string td k;hx x);};
run:{[r] k:r 0; v:r 1; o:r 2; c:r 3;
  {[k;v;o;c;i] emit[k;"atom";c i;o i;v i]}[k;v;o;c] each til count v;
  vec:(v where o=`ok),v first where o=`null;     / normal values plus a typed null
  emit[k;"vector";"ok+null";`ok;vec];
  emit[k;"table";"1 col";`ok;([] x:vec)];};
-1 "kind|form|case|outcome|qtype|expected_parity|expected_today|hex";
run each L;
exit 0
