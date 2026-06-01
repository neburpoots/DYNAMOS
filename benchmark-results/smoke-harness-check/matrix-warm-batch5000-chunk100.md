| dataset | limit | workload | archetype | providers | temperature | transport | response mode | batch | chunk | ok/runs | first median (min-max) | done median (min-max) | rows | result match |
| --- | ---: | --- | --- | --- | --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| large | 5000 | bulk | dataThroughTtp | UVA | warm | unary | batched | 5000 | 100 | 1/1 | 3.568 (3.568-3.568) | 3.568 (3.568-3.568) | 5000.0 | no |
| large | 5000 | bulk | dataThroughTtp | UVA | warm | unary | classic-unary | 5000 | 100 | 1/1 | 3.481 (3.481-3.481) | 3.481 (3.481-3.481) | 5000.0 | yes |
| large | 5000 | bulk | dataThroughTtp | UVA | warm | streaming | batched | 5000 | 100 | 1/1 | 4.252 (4.252-4.252) | 4.252 (4.252-4.252) | 5000.0 | no |
| large | 5000 | bulk | dataThroughTtp | UVA | warm | rabbitmq-streams | batched | 5000 | 100 | 1/1 | 3.701 (3.701-3.701) | 3.702 (3.702-3.702) | 5000.0 | no |
| large | 5000 | bulk | dataThroughTtp | UVA,VU | warm | unary | batched | 5000 | 100 | 1/1 | 4.287 (4.287-4.287) | 4.287 (4.287-4.287) | 10000.0 | no |
| large | 5000 | bulk | dataThroughTtp | UVA,VU | warm | unary | classic-unary | 5000 | 100 | 1/1 | 4.684 (4.684-4.684) | 4.684 (4.684-4.684) | 10000.0 | yes |
| large | 5000 | bulk | dataThroughTtp | UVA,VU | warm | streaming | batched | 5000 | 100 | 1/1 | 4.747 (4.747-4.747) | 4.747 (4.747-4.747) | 10000.0 | no |
| large | 5000 | bulk | dataThroughTtp | UVA,VU | warm | rabbitmq-streams | batched | 5000 | 100 | 1/1 | 4.796 (4.796-4.796) | 4.796 (4.796-4.796) | 10000.0 | no |
| large | 5000 | average | dataThroughTtp | UVA | warm | unary | batched | 5000 | 100 | 1/1 | 3.312 (3.312-3.312) | 3.312 (3.312-3.312) | 5000.0 | yes |
| large | 5000 | average | dataThroughTtp | UVA | warm | unary | classic-unary | 5000 | 100 | 1/1 | 3.215 (3.215-3.215) | 3.215 (3.215-3.215) | None | yes |
| large | 5000 | average | dataThroughTtp | UVA | warm | streaming | batched | 5000 | 100 | 1/1 | 3.326 (3.326-3.326) | 3.326 (3.326-3.326) | 5000.0 | yes |
| large | 5000 | average | dataThroughTtp | UVA | warm | rabbitmq-streams | batched | 5000 | 100 | 1/1 | 3.916 (3.916-3.916) | 3.916 (3.916-3.916) | 5000.0 | yes |
| large | 5000 | average | dataThroughTtp | UVA,VU | warm | unary | batched | 5000 | 100 | 1/1 | 3.746 (3.746-3.746) | 3.762 (3.762-3.762) | 10000.0 | yes |
| large | 5000 | average | dataThroughTtp | UVA,VU | warm | unary | classic-unary | 5000 | 100 | 1/1 | 4.485 (4.485-4.485) | 4.485 (4.485-4.485) | None | yes |
| large | 5000 | average | dataThroughTtp | UVA,VU | warm | streaming | batched | 5000 | 100 | 1/1 | 3.256 (3.256-3.256) | 3.472 (3.472-3.472) | 10000.0 | yes |
| large | 5000 | average | dataThroughTtp | UVA,VU | warm | rabbitmq-streams | batched | 5000 | 100 | 1/1 | 4.541 (4.541-4.541) | 4.586 (4.586-4.586) | 10000.0 | yes |
