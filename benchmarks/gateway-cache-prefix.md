# Gateway stable-prefix caching experiment

An explicit cache boundary before mutable runtime context preserved the stable instruction prefix on Claude Haiku, Opus, and Sonnet when the workspace branch changed. These measurements do not establish a general latency improvement or an improvement for the non-Anthropic vendors.

## Method

The initial five-model experiment made 30 live requests through fx's native Vercel AI Gateway adapter: five model vendors, three turns before the cache-boundary patch and three after it. Each sequence used an isolated session and a unique synthetic system prefix:

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

Haiku's changed-context request read 34,259 tokens from cache after the patch, compared with zero before it. Its reported model cost fell from $0.0043579 to $0.00041874, while process time increased from 2.110s to 7.303s.

Gateway metadata summed to approximately $0.236 in `cost` and $0.239 in `gatewayCost` for all 30 requests. These fields are reported metadata, not an invoice. DeepSeek's resolved provider charged more than the public model catalog estimate.

## Opus and Sonnet follow-up

A follow-up made 12 live requests with `anthropic/claude-opus-5.5` and `anthropic/claude-sonnet-5.5`, selected from the live Gateway catalog. Both arms ran this checkout's built `./zig-out/bin/fx` at commit `cfa5de350e0bffc0eca4bfa7d962bf84770a272d`. For the baseline, the observer removed only the explicit stable-instruction cache marker. The improved arm retained it. Automatic Gateway caching, message roles and order, live runtime context, and native session affinity remained unchanged.

The two models ran in parallel. Within each model, the three baseline turns preceded the three improved turns, using separate sessions and unique synthetic system prefixes. The follow-up reused the pinned public source, context-freshness question, branch change, low reasoning effort, disabled Fast and Ultrafast, 1,024-token output cap, and privacy filtering described above. Each model was limited to six paid requests, with no model tools or paid retries.

Before billing, both six-turn loopback preflights passed. After normalizing the unique synthetic prefix and removing the improved marker, all three request pairs for each model were identical.

| Model | Unchanged-context cache read, baseline / improved | Changed-context cache read, baseline / improved | Changed-context process time, baseline / improved | Changed-context reported model cost, baseline / improved |
| --- | ---: | ---: | ---: | ---: |
| `anthropic/claude-opus-5.5` | 99.89% / 99.89% | 0% / 98.32% | 4.775s / 3.058s | $0.174316 / $0.009897 |
| `anthropic/claude-sonnet-5.5` | 99.89% / 99.89% | 0% / 98.32% | 4.799s / 2.311s | $0.0871705 / $0.0049487 |

Both models resolved to Anthropic. All 12 answers named the current branch, all processes exited successfully with clean stderr, and fx's cache counters matched Gateway on every request.

On the changed-context turn, Opus read 34,255 of 34,841 input tokens from cache with the marker, compared with zero of 34,840 without it. Cache writes fell from 34,836 to 582 tokens. Sonnet read 34,257 of 34,843 input tokens from cache with the marker, compared with zero of 34,845 without it; cache writes fell from 34,841 to 582 tokens.

For these 12 requests, Gateway metadata summed to approximately $0.820 in `cost` and $0.821 in `gatewayCost`. These are reported metadata, not an invoice. The changed-context process times fell in this sample, but the models ran concurrently and each condition was measured only once. The follow-up supports cache retention on Opus and Sonnet, not a general latency guarantee.

## Failures and limits

- In the initial five-model experiment, 29 of 30 answers named the current branch. Gemini returned the previous branch on the changed-context turn after the patch, despite receiving `git_branch: cache-next` and reporting zero cache-read tokens. The patch does not resolve that response failure or Gemini's cache miss.
- Only Anthropic uses the explicit marker added by this patch. The other tested vendors remain on their existing implicit caching behavior. DeepSeek varied in both directions, so its changed-context gain is not attributed to the patch.
- There was one three-turn sequence per model per arm, with no randomized ordering or statistical confidence. The measurements do not establish a latency improvement.
- The unique system prefix separated each sequence, but common tool definitions could already be cached. The changed OpenAI arm's initial request read 3,584 cached tokens.
- Cache thresholds, expiry, routing, and provider capacity still affect hit rates. An explicit boundary does not guarantee a cache hit or make all input tokens cacheable.

The implementation follows the documented [Anthropic message-level cache control](https://ai-sdk.dev/providers/ai-sdk-providers/anthropic#cache-control) while retaining [Gateway automatic caching](https://vercel.com/docs/ai-gateway/models-and-providers/automatic-caching). It does not move trusted runtime instructions into conversation history or freeze environment values to improve cache metrics.
