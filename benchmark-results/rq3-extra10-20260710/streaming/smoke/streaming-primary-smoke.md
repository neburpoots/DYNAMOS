| dataset | limit | workload | query shape | archetype | providers | temperature | transport | response mode | batch | chunk | ok/runs | first median (min-max) | done median (min-max) | rows | content match | raw match |
| --- | ---: | --- | --- | --- | --- | --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | --- | --- |
| large | 5000 | bulk | default | dataThroughTtp | UVA | warm | unary | batched | 5000 | 100 | 1/1 | 4.076 (4.076-4.076) | 4.076 (4.076-4.076) | 5000.0 | yes | yes |
| large | 5000 | bulk | default | dataThroughTtp | UVA | warm | streaming | batched | 5000 | 100 | 1/1 | 3.137 (3.137-3.137) | 3.137 (3.137-3.137) | 5000.0 | yes | yes |
| large | 5000 | bulk | default | dataThroughTtp | UVA | warm | rabbitmq-streams | batched | 5000 | 100 | 1/1 | 4.349 (4.349-4.349) | 4.349 (4.349-4.349) | 5000.0 | yes | yes |
