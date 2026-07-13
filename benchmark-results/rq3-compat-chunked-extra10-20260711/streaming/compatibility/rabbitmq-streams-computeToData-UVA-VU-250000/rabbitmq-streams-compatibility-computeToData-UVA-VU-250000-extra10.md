| dataset | limit | workload | query shape | archetype | providers | temperature | transport | response mode | batch | chunk | ok/runs | first median (min-max) | done median (min-max) | rows | content match | raw match |
| --- | ---: | --- | --- | --- | --- | --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | --- | --- |
| large | 250000 | bulk | default | computeToData | UVA,VU | warm | rabbitmq-streams | batched | 5000 | 100 | 10/10 | 3.158 (2.181-5.248) | 10.212 (9.282-14.626) | 500000.0 | yes | yes |
