| dataset | limit | archetype | transport | batch | chunk | ok/runs | first median (min-max) | done median (min-max) | rows | result match |
| --- | ---: | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| large | 1000 | dataThroughTtp | unary | 5000 | 100 | 1/1 | 4.199 (4.199-4.199) | 4.203 (4.203-4.203) | 2000.0 | yes |
| large | 1000 | dataThroughTtp | streaming | 5000 | 100 | 1/1 | 3.529 (3.529-3.529) | 3.529 (3.529-3.529) | 2000.0 | yes |
| large | 1000 | dataThroughTtp | rabbitmq-streams | 5000 | 100 | 1/1 | 4.681 (4.681-4.681) | 4.729 (4.729-4.729) | 2000.0 | yes |
