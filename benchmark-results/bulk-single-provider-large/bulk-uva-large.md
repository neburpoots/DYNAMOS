| dataset | limit | workload | archetype | providers | temperature | transport | response mode | batch | chunk | ok/runs | first median (min-max) | done median (min-max) | rows | result match |
| --- | ---: | --- | --- | --- | --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| large | 50000 | bulk | dataThroughTtp | UVA | warm | unary | batched | 5000 | 100 | 1/1 | 4.194 (4.194-4.194) | 4.195 (4.195-4.195) | 50000.0 | no |
| large | 50000 | bulk | dataThroughTtp | UVA | warm | unary | classic-unary | 5000 | 100 | 1/1 | 6.793 (6.793-6.793) | 6.793 (6.793-6.793) | 50000.0 | yes |
| large | 50000 | bulk | dataThroughTtp | UVA | warm | streaming | batched | 5000 | 100 | 1/1 | 4.257 (4.257-4.257) | 4.257 (4.257-4.257) | 50000.0 | no |
| large | 50000 | bulk | dataThroughTtp | UVA | warm | rabbitmq-streams | batched | 5000 | 100 | 1/1 | 6.001 (6.001-6.001) | 6.002 (6.002-6.002) | 50000.0 | no |
| large | 250000 | bulk | dataThroughTtp | UVA | warm | unary | batched | 5000 | 100 | 0/1 | None (None-None) | None (None-None) | None | yes |
| large | 250000 | bulk | dataThroughTtp | UVA | warm | unary | classic-unary | 5000 | 100 | 0/1 | None (None-None) | None (None-None) | None | yes |
| large | 250000 | bulk | dataThroughTtp | UVA | warm | streaming | batched | 5000 | 100 | 0/1 | None (None-None) | None (None-None) | None | yes |
| large | 250000 | bulk | dataThroughTtp | UVA | warm | rabbitmq-streams | batched | 5000 | 100 | 0/1 | None (None-None) | None (None-None) | None | yes |
