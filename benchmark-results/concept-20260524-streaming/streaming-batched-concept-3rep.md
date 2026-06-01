| dataset | limit | workload | archetype | providers | temperature | transport | response mode | batch | chunk | ok/runs | first median (min-max) | done median (min-max) | rows | content match | raw match |
| --- | ---: | --- | --- | --- | --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | --- | --- |
| large | 50000 | bulk | dataThroughTtp | UVA | warm | unary | batched | 5000 | 100 | 3/3 | 7.627 (6.892-8.453) | 8.066 (7.666-9.05) | 50000.0 | yes | yes |
| large | 50000 | bulk | dataThroughTtp | UVA | warm | streaming | batched | 5000 | 100 | 3/3 | 6.651 (5.362-14.75) | 7.17 (6.285-16.722) | 50000.0 | yes | yes |
| large | 50000 | bulk | dataThroughTtp | UVA | warm | rabbitmq-streams | batched | 5000 | 100 | 3/3 | 5.608 (5.154-5.99) | 7.053 (6.692-8.4) | 50000.0 | yes | yes |
| large | 100000 | bulk | dataThroughTtp | UVA | warm | unary | batched | 5000 | 100 | 3/3 | 6.741 (5.829-11.4) | 8.564 (7.87-16.19) | 100000.0 | yes | yes |
| large | 100000 | bulk | dataThroughTtp | UVA | warm | streaming | batched | 5000 | 100 | 3/3 | 6.121 (5.973-9.217) | 7.892 (7.436-11.262) | 100000.0 | yes | yes |
| large | 100000 | bulk | dataThroughTtp | UVA | warm | rabbitmq-streams | batched | 5000 | 100 | 3/3 | 6.834 (5.864-7.102) | 8.854 (8.236-9.253) | 100000.0 | yes | yes |
| large | 250000 | bulk | dataThroughTtp | UVA | warm | unary | batched | 5000 | 100 | 3/3 | 8.256 (6.964-13.7) | 16.634 (11.771-18.785) | 250000.0 | yes | yes |
| large | 250000 | bulk | dataThroughTtp | UVA | warm | streaming | batched | 5000 | 100 | 3/3 | 5.667 (5.435-6.575) | 11.778 (10.605-12.188) | 250000.0 | yes | yes |
| large | 250000 | bulk | dataThroughTtp | UVA | warm | rabbitmq-streams | batched | 5000 | 100 | 3/3 | 7.275 (6.119-12.41) | 24.317 (13.608-30.493) | 250000.0 | yes | yes |
