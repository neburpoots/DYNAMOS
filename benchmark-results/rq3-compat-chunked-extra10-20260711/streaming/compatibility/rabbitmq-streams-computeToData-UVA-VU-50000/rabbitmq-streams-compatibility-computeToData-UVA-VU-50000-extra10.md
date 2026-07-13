| dataset | limit | workload | query shape | archetype | providers | temperature | transport | response mode | batch | chunk | ok/runs | first median (min-max) | done median (min-max) | rows | content match | raw match |
| --- | ---: | --- | --- | --- | --- | --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | --- | --- |
| large | 50000 | bulk | default | computeToData | UVA,VU | warm | rabbitmq-streams | batched | 5000 | 100 | 10/10 | 3.134 (2.185-3.369) | 4.42 (4.19-5.04) | 100000.0 | yes | yes |
