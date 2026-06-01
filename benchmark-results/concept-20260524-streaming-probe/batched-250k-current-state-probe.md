| dataset | limit | workload | archetype | providers | temperature | transport | response mode | batch | chunk | ok/runs | first median (min-max) | done median (min-max) | rows | content match | raw match |
| --- | ---: | --- | --- | --- | --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | --- | --- |
| large | 250000 | bulk | dataThroughTtp | UVA | warm | unary | batched | 5000 | 100 | 1/1 | 6.493 (6.493-6.493) | 12.272 (12.272-12.272) | 250000.0 | yes | yes |
| large | 250000 | bulk | dataThroughTtp | UVA | warm | streaming | batched | 5000 | 100 | 1/1 | 7.817 (7.817-7.817) | 10.823 (10.823-10.823) | 250000.0 | yes | yes |
| large | 250000 | bulk | dataThroughTtp | UVA | warm | rabbitmq-streams | batched | 5000 | 100 | 1/1 | 6.875 (6.875-6.875) | 14.609 (14.609-14.609) | 250000.0 | yes | yes |
