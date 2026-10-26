# Gateway stable-prefix caching experiment

An explicit cache boundary before mutable runtime context preserved Claude's stable instruction prefix when the workspace branch changed. This experiment does not establish a latency improvement or an improvement for the other tested vendors.

## Method

The experiment made 30 live requests through fx's native Vercel AI Gateway adapter: five model vendors, three turns before the cache-boundary patch and three after it. Each sequence used an isolated session and a unique synthetic system prefix:

1. Start with a long stable instruction containing public Vercel AI SDK source.
2. Resume the session without changing the workspace context.
3. Change the synthetic workspace's actual Git branch and resume again.

Each prompt asked for the current branch from `fx-turn-context`. The source provided realistic input size, but the task was a synthetic context-freshness check, not a coding-quality evaluation. The public source was pinned to [vercel/ai revision 20c4031d1fc59aab54b67c16679e3981374690ab](https://github.com/vercel/ai/tree/20c4031d1fc59aab54b67c16679e3981374690ab).

Both arms used low reasoning effort, disabled Fast and Ultrafast, and retained automatic Gateway caching and native session affinity. A local observer capped output at 1,024 tokens, required disabled model tools, allowed at most 30 upstream requests, and made no paid retries. It replaced private environment values and the local skills inventory with synthetic data without moving or dropping the runtime-context message. Authentication headers went only to Gateway and were not included in the report.

All baseline sequences preceded the changed sequences. The live comparison used a development checkout with cache-usage instrumentation before the contribution was isolated into its PR branch. These measurements are not a live benchmark of the final PR commit.

## Results

Cache-read percentages are the share of input tokens read from cache, not request success rates. Process time includes the local fx process and observer, not only provider generation time.

| Model | Unchanged-context cache read, before / after | Changed-context cache read, before / after | Changed-context process time, before / after |
| --- | ---: | ---: | ---: |
| `deepseek/deepseek-v4.1-flash` | 99.05% / 87.19% | 87.06% / 97.49% | 2.273s / 2.568s |
| `moonshotai/kimi-k3.1-pro` | 98.24% / 98.31% | 96.63% / 96.70% | 7.752s / 5.658s |
| `anthropic/claude-haiku-5.5` | 99.89% / 99.89% | 0% / 98.32% | 2.110s / 7.303s |
| `openai/gpt-5.4-mini` | 92.03% / 92.03% | 91.90% / 91.89% | 2.253s / 2.155s |
| `google/gemini-3.8-flash` | 67.59% / 67.62% | 0% / 0% | 6.404s / 3.453s |

The resolved providers were Fireworks, Moonshot AI, Anthropic, OpenAI, and Vertex, respectively. fx's cache counters matched Gateway on every request. All 30 processes exited successfully with clean stderr.

Claude's changed-context request read 34,259 tokens from cache after the patch, compared with zero before it. Its reported model cost fell from $0.0043579 to $0.00041874, while process time increased from 2.110s to 7.303s.

Gateway metadata summed to approximately $0.236 in `cost` and $0.239 in `gatewayCost` for all 30 requests. These fields are reported metadata, not an invoice. DeepSeek's resolved provider charged more than the public model catalog estimate.

## Failures and limits

- 29 of 30 answers named the current branch. Gemini returned the previous branch on the changed-context turn after the patch, despite receiving `git_branch: cache-next` and reporting zero cache-read tokens. The patch does not resolve that response failure or Gemini's cache miss.
- Only Anthropic uses the explicit marker added by this patch. The other tested vendors remain on their existing implicit caching behavior. DeepSeek varied in both directions, so its changed-context gain is not attributed to the patch.
- There was one three-turn sequence per model per arm, with no randomized ordering or statistical confidence. The measurements do not establish a latency improvement.
- The unique system prefix separated each sequence, but common tool definitions could already be cached. The changed OpenAI arm's initial request read 3,584 cached tokens.
- Cache thresholds, expiry, routing, and provider capacity still affect hit rates. An explicit boundary does not guarantee a cache hit or make all input tokens cacheable.

The implementation follows the documented [Anthropic message-level cache control](https://ai-sdk.dev/providers/ai-sdk-providers/anthropic#cache-control) while retaining [Gateway automatic caching](https://vercel.com/docs/ai-gateway/models-and-providers/automatic-caching). It does not move trusted runtime instructions into conversation history or freeze environment values to improve cache metrics.
