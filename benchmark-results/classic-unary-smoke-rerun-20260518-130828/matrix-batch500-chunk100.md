| dataset | limit | archetype | transport | response mode | batch | chunk | ok/runs | first median (min-max) | done median (min-max) | rows | result match |
| --- | ---: | --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| large | 2000 | dataThroughTtp | unary | batched | 500 | 100 | 1/1 | 4.247 (4.247-4.247) | 4.31 (4.31-4.31) | 4000.0 | yes |
| large | 2000 | dataThroughTtp | unary | classic-unary | 500 | 100 | 1/1 | 3.338 (3.338-3.338) | 3.344 (3.344-3.344) | 4000.0 | yes |
| large | 2000 | dataThroughTtp | streaming | batched | 500 | 100 | 1/1 | 3.212 (3.212-3.212) | 3.215 (3.215-3.215) | 4000.0 | yes |
| large | 2000 | dataThroughTtp | rabbitmq-streams | batched | 500 | 100 | 1/1 | 4.453 (4.453-4.453) | 4.803 (4.803-4.803) | 4000.0 | yes |
| large | 2000 | computeToData | unary | batched | 500 | 100 | 1/1 | 2.083 (2.083-2.083) | 3.063 (3.063-3.063) | 4000.0 | yes |
| large | 2000 | computeToData | unary | classic-unary | 500 | 100 | 1/1 | 3.026 (3.026-3.026) | 3.04 (3.04-3.04) | 4000.0 | yes |
| large | 2000 | computeToData | streaming | batched | 500 | 100 | 1/1 | 2.979 (2.979-2.979) | 4.231 (4.231-4.231) | 4000.0 | yes |
| large | 2000 | computeToData | rabbitmq-streams | batched | 500 | 100 | 1/1 | 3.03 (3.03-3.03) | 4.152 (4.152-4.152) | 4000.0 | yes |
