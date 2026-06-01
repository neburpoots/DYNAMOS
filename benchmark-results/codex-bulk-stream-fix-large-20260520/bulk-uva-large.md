| dataset | limit | workload | archetype | providers | temperature | transport | response mode | batch | chunk | ok/runs | first median (min-max) | done median (min-max) | rows | content match | raw match |
| --- | ---: | --- | --- | --- | --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | --- | --- |
| large | 50000 | bulk | dataThroughTtp | UVA | warm | unary | batched | 5000 | 100 | 1/1 | 3.089 (3.089-3.089) | 3.567 (3.567-3.567) | 50000.0 | yes | yes |
| large | 50000 | bulk | dataThroughTtp | UVA | warm | unary | classic-unary | 5000 | 100 | 1/1 | 3.716 (3.716-3.716) | 3.716 (3.716-3.716) | 50000.0 | yes | yes |
| large | 50000 | bulk | dataThroughTtp | UVA | warm | streaming | batched | 5000 | 100 | 1/1 | 2.903 (2.903-2.903) | 3.073 (3.073-3.073) | 50000.0 | yes | yes |
| large | 50000 | bulk | dataThroughTtp | UVA | warm | rabbitmq-streams | batched | 5000 | 100 | 1/1 | 2.931 (2.931-2.931) | 3.676 (3.676-3.676) | 50000.0 | yes | yes |
| large | 250000 | bulk | dataThroughTtp | UVA | warm | unary | batched | 5000 | 100 | 1/1 | 4.108 (4.108-4.108) | 8.941 (8.941-8.941) | 250000.0 | yes | yes |
| large | 250000 | bulk | dataThroughTtp | UVA | warm | unary | classic-unary | 5000 | 100 | 0/1 | None (None-None) | None (None-None) | None | yes | yes |
| large | 250000 | bulk | dataThroughTtp | UVA | warm | streaming | batched | 5000 | 100 | 1/1 | 2.865 (2.865-2.865) | 6.043 (6.043-6.043) | 250000.0 | yes | yes |
| large | 250000 | bulk | dataThroughTtp | UVA | warm | rabbitmq-streams | batched | 5000 | 100 | 1/1 | 3.526 (3.526-3.526) | 10.023 (10.023-10.023) | 250000.0 | yes | yes |
