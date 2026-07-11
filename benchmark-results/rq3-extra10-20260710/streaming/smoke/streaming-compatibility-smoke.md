| dataset | limit | workload | query shape | archetype | providers | temperature | transport | response mode | batch | chunk | ok/runs | first median (min-max) | done median (min-max) | rows | content match | raw match |
| --- | ---: | --- | --- | --- | --- | --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | --- | --- |
| large | 10000 | bulk | default | dataThroughTtp | UVA,VU | warm | unary | batched | 5000 | 100 | 1/1 | 3.171 (3.171-3.171) | 3.294 (3.294-3.294) | 20000.0 | yes | no |
| large | 10000 | bulk | default | dataThroughTtp | UVA,VU | warm | streaming | batched | 5000 | 100 | 1/1 | 3.101 (3.101-3.101) | 3.139 (3.139-3.139) | 20000.0 | yes | no |
| large | 10000 | bulk | default | dataThroughTtp | UVA,VU | warm | rabbitmq-streams | batched | 5000 | 100 | 1/1 | 3.505 (3.505-3.505) | 4.222 (4.222-4.222) | 20000.0 | yes | no |
| large | 10000 | bulk | default | computeToData | UVA,VU | warm | unary | batched | 5000 | 100 | 1/1 | 2.984 (2.984-2.984) | 3.119 (3.119-3.119) | 20000.0 | yes | yes |
| large | 10000 | bulk | default | computeToData | UVA,VU | warm | streaming | batched | 5000 | 100 | 1/1 | 3.039 (3.039-3.039) | 4.075 (4.075-4.075) | 20000.0 | yes | yes |
| large | 10000 | bulk | default | computeToData | UVA,VU | warm | rabbitmq-streams | batched | 5000 | 100 | 1/1 | 3.103 (3.103-3.103) | 3.275 (3.275-3.275) | 20000.0 | yes | yes |
