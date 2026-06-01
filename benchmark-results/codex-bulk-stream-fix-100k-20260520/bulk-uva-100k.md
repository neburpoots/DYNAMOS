| dataset | limit | workload | archetype | providers | temperature | transport | response mode | batch | chunk | ok/runs | first median (min-max) | done median (min-max) | rows | content match | raw match |
| --- | ---: | --- | --- | --- | --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | --- | --- |
| large | 100000 | bulk | dataThroughTtp | UVA | warm | unary | batched | 5000 | 100 | 1/1 | 3.059 (3.059-3.059) | 4.274 (4.274-4.274) | 100000.0 | yes | yes |
| large | 100000 | bulk | dataThroughTtp | UVA | warm | unary | classic-unary | 5000 | 100 | 0/1 | None (None-None) | None (None-None) | None | yes | yes |
| large | 100000 | bulk | dataThroughTtp | UVA | warm | streaming | batched | 5000 | 100 | 1/1 | 3.988 (3.988-3.988) | 5.281 (5.281-5.281) | 100000.0 | yes | yes |
| large | 100000 | bulk | dataThroughTtp | UVA | warm | rabbitmq-streams | batched | 5000 | 100 | 1/1 | 2.424 (2.424-2.424) | 4.948 (4.948-4.948) | 100000.0 | yes | yes |
