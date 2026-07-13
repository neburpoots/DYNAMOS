| dataset | limit | workload | query shape | archetype | providers | temperature | transport | response mode | batch | chunk | ok/runs | first median (min-max) | done median (min-max) | rows | content match | raw match |
| --- | ---: | --- | --- | --- | --- | --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | --- | --- |
| large | 50000 | bulk | default | dataThroughTtp | UVA,VU | warm | rabbitmq-streams | batched | 5000 | 100 | 10/10 | 3.974 (3.538-5.136) | 5.769 (4.668-7.501) | 100000.0 | yes | no |
