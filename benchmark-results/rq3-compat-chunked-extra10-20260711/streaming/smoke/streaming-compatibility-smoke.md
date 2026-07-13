| dataset | limit | workload | query shape | archetype | providers | temperature | transport | response mode | batch | chunk | ok/runs | first median (min-max) | done median (min-max) | rows | content match | raw match |
| --- | ---: | --- | --- | --- | --- | --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | --- | --- |
| large | 10000 | bulk | default | dataThroughTtp | UVA,VU | warm | unary | batched | 5000 | 100 | 1/1 | 3.324 (3.324-3.324) | 3.489 (3.489-3.489) | 20000.0 | yes | no |
| large | 10000 | bulk | default | dataThroughTtp | UVA,VU | warm | streaming | batched | 5000 | 100 | 1/1 | 4.185 (4.185-4.185) | 4.224 (4.224-4.224) | 20000.0 | yes | no |
| large | 10000 | bulk | default | dataThroughTtp | UVA,VU | warm | rabbitmq-streams | batched | 5000 | 100 | 1/1 | 6.052 (6.052-6.052) | 6.297 (6.297-6.297) | 20000.0 | yes | no |
| large | 10000 | bulk | default | computeToData | UVA,VU | warm | unary | batched | 5000 | 100 | 1/1 | 2.932 (2.932-2.932) | 3.552 (3.552-3.552) | 20000.0 | yes | yes |
| large | 10000 | bulk | default | computeToData | UVA,VU | warm | streaming | batched | 5000 | 100 | 1/1 | 3.058 (3.058-3.058) | 3.071 (3.071-3.071) | 20000.0 | yes | yes |
| large | 10000 | bulk | default | computeToData | UVA,VU | warm | rabbitmq-streams | batched | 5000 | 100 | 1/1 | 6.765 (6.765-6.765) | 7.748 (7.748-7.748) | 20000.0 | yes | yes |
