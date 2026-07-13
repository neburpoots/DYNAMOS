| dataset | limit | workload | query shape | archetype | providers | temperature | transport | response mode | batch | chunk | ok/runs | first median (min-max) | done median (min-max) | rows | content match | raw match |
| --- | ---: | --- | --- | --- | --- | --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | --- | --- |
| large | 50000 | bulk | default | dataThroughTtp | UVA,VU | warm | rabbitmq-streams | batched | 5000 | 100 | 10/10 | 5.088 (4.365-7.345) | 7.284 (5.448-9.402) | 100000.0 | yes | no |
