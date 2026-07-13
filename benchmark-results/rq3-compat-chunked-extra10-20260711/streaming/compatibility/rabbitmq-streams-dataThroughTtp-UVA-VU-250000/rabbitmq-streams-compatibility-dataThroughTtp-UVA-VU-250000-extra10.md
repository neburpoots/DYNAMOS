| dataset | limit | workload | query shape | archetype | providers | temperature | transport | response mode | batch | chunk | ok/runs | first median (min-max) | done median (min-max) | rows | content match | raw match |
| --- | ---: | --- | --- | --- | --- | --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | --- | --- |
| large | 250000 | bulk | default | dataThroughTtp | UVA,VU | warm | rabbitmq-streams | batched | 5000 | 100 | 10/10 | 4.407 (3.375-5.852) | 12.79 (11.288-19.894) | 500000.0 | yes | no |
