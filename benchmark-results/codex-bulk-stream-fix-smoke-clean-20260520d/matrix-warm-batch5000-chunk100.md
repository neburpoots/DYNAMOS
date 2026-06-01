| dataset | limit | workload | archetype | providers | temperature | transport | response mode | batch | chunk | ok/runs | first median (min-max) | done median (min-max) | rows | content match | raw match |
| --- | ---: | --- | --- | --- | --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | --- | --- |
| large | 5000 | bulk | dataThroughTtp | UVA | warm | unary | batched | 5000 | 100 | 0/1 | None (None-None) | None (None-None) | None | yes | yes |
| large | 5000 | bulk | dataThroughTtp | UVA | warm | unary | classic-unary | 5000 | 100 | 1/1 | 2.895 (2.895-2.895) | 2.895 (2.895-2.895) | 5000.0 | yes | yes |
| large | 5000 | bulk | dataThroughTtp | UVA | warm | streaming | batched | 5000 | 100 | 0/1 | None (None-None) | None (None-None) | None | yes | yes |
| large | 5000 | bulk | dataThroughTtp | UVA | warm | rabbitmq-streams | batched | 5000 | 100 | 0/1 | None (None-None) | None (None-None) | None | yes | yes |
| large | 5000 | bulk | dataThroughTtp | UVA,VU | warm | unary | batched | 5000 | 100 | 1/1 | 4.128 (4.128-4.128) | 4.161 (4.161-4.161) | 10000.0 | yes | no |
| large | 5000 | bulk | dataThroughTtp | UVA,VU | warm | unary | classic-unary | 5000 | 100 | 1/1 | 2.984 (2.984-2.984) | 2.984 (2.984-2.984) | 10000.0 | yes | yes |
| large | 5000 | bulk | dataThroughTtp | UVA,VU | warm | streaming | batched | 5000 | 100 | 1/1 | 3.233 (3.233-3.233) | 3.24 (3.24-3.24) | 10000.0 | yes | no |
| large | 5000 | bulk | dataThroughTtp | UVA,VU | warm | rabbitmq-streams | batched | 5000 | 100 | 1/1 | 4.133 (4.133-4.133) | 4.182 (4.182-4.182) | 10000.0 | yes | no |
| large | 5000 | average | dataThroughTtp | UVA | warm | unary | batched | 5000 | 100 | 0/1 | None (None-None) | None (None-None) | None | yes | yes |
| large | 5000 | average | dataThroughTtp | UVA | warm | unary | classic-unary | 5000 | 100 | 1/1 | 2.91 (2.91-2.91) | 2.91 (2.91-2.91) | None | yes | yes |
| large | 5000 | average | dataThroughTtp | UVA | warm | streaming | batched | 5000 | 100 | 0/1 | None (None-None) | None (None-None) | None | yes | yes |
| large | 5000 | average | dataThroughTtp | UVA | warm | rabbitmq-streams | batched | 5000 | 100 | 0/1 | None (None-None) | None (None-None) | None | yes | yes |
| large | 5000 | average | dataThroughTtp | UVA,VU | warm | unary | batched | 5000 | 100 | 1/1 | 4.018 (4.018-4.018) | 4.029 (4.029-4.029) | 10000.0 | yes | yes |
| large | 5000 | average | dataThroughTtp | UVA,VU | warm | unary | classic-unary | 5000 | 100 | 1/1 | 2.983 (2.983-2.983) | 2.983 (2.983-2.983) | None | yes | yes |
| large | 5000 | average | dataThroughTtp | UVA,VU | warm | streaming | batched | 5000 | 100 | 1/1 | 3.971 (3.971-3.971) | 3.976 (3.976-3.976) | 10000.0 | yes | yes |
| large | 5000 | average | dataThroughTtp | UVA,VU | warm | rabbitmq-streams | batched | 5000 | 100 | 1/1 | 4.091 (4.091-4.091) | 4.141 (4.141-4.141) | 10000.0 | yes | yes |
