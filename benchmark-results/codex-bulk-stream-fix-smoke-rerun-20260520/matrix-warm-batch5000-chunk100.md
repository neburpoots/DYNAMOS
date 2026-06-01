| dataset | limit | workload | archetype | providers | temperature | transport | response mode | batch | chunk | ok/runs | first median (min-max) | done median (min-max) | rows | content match | raw match |
| --- | ---: | --- | --- | --- | --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | --- | --- |
| large | 5000 | bulk | dataThroughTtp | UVA | warm | unary | batched | 5000 | 100 | 1/1 | 4.036 (4.036-4.036) | 4.036 (4.036-4.036) | 5000.0 | yes | yes |
| large | 5000 | bulk | dataThroughTtp | UVA | warm | unary | classic-unary | 5000 | 100 | 1/1 | 2.832 (2.832-2.832) | 2.832 (2.832-2.832) | 5000.0 | yes | yes |
| large | 5000 | bulk | dataThroughTtp | UVA | warm | streaming | batched | 5000 | 100 | 1/1 | 2.853 (2.853-2.853) | 2.854 (2.854-2.854) | 5000.0 | yes | yes |
| large | 5000 | bulk | dataThroughTtp | UVA | warm | rabbitmq-streams | batched | 5000 | 100 | 1/1 | 3.07 (3.07-3.07) | 3.07 (3.07-3.07) | 5000.0 | yes | yes |
| large | 5000 | bulk | dataThroughTtp | UVA,VU | warm | unary | batched | 5000 | 100 | 1/1 | 2.992 (2.992-2.992) | 3.037 (3.037-3.037) | 10000.0 | yes | no |
| large | 5000 | bulk | dataThroughTtp | UVA,VU | warm | unary | classic-unary | 5000 | 100 | 1/1 | 3.977 (3.977-3.977) | 3.977 (3.977-3.977) | 10000.0 | yes | yes |
| large | 5000 | bulk | dataThroughTtp | UVA,VU | warm | streaming | batched | 5000 | 100 | 1/1 | 3.884 (3.884-3.884) | 3.905 (3.905-3.905) | 10000.0 | yes | no |
| large | 5000 | bulk | dataThroughTtp | UVA,VU | warm | rabbitmq-streams | batched | 5000 | 100 | 1/1 | 3.106 (3.106-3.106) | 3.151 (3.151-3.151) | 10000.0 | yes | no |
| large | 5000 | average | dataThroughTtp | UVA | warm | unary | batched | 5000 | 100 | 1/1 | 2.944 (2.944-2.944) | 2.944 (2.944-2.944) | 5000.0 | yes | yes |
| large | 5000 | average | dataThroughTtp | UVA | warm | unary | classic-unary | 5000 | 100 | 1/1 | 2.975 (2.975-2.975) | 2.975 (2.975-2.975) | None | yes | yes |
| large | 5000 | average | dataThroughTtp | UVA | warm | streaming | batched | 5000 | 100 | 1/1 | 2.8 (2.8-2.8) | 2.8 (2.8-2.8) | 5000.0 | yes | yes |
| large | 5000 | average | dataThroughTtp | UVA | warm | rabbitmq-streams | batched | 5000 | 100 | 1/1 | 3.018 (3.018-3.018) | 3.018 (3.018-3.018) | 5000.0 | yes | yes |
| large | 5000 | average | dataThroughTtp | UVA,VU | warm | unary | batched | 5000 | 100 | 1/1 | 2.993 (2.993-2.993) | 3.007 (3.007-3.007) | 10000.0 | yes | yes |
| large | 5000 | average | dataThroughTtp | UVA,VU | warm | unary | classic-unary | 5000 | 100 | 1/1 | 2.782 (2.782-2.782) | 2.782 (2.782-2.782) | None | yes | yes |
| large | 5000 | average | dataThroughTtp | UVA,VU | warm | streaming | batched | 5000 | 100 | 1/1 | 2.91 (2.91-2.91) | 2.913 (2.913-2.913) | 10000.0 | yes | yes |
| large | 5000 | average | dataThroughTtp | UVA,VU | warm | rabbitmq-streams | batched | 5000 | 100 | 1/1 | 4.151 (4.151-4.151) | 4.2 (4.2-4.2) | 10000.0 | yes | yes |
