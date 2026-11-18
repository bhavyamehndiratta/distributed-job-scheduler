package io.github.bhavyamehndiratta.djs.scheduler;

import java.util.HashMap;
import java.util.Map;

/** Built-in handlers, enough for tests and benchmarks. */
public final class Handlers {

    private Handlers() {
    }

    private static int ticks(String args, int fallback) {
        try {
            return Math.max(1, Integer.parseInt(args.trim()));
        } catch (NumberFormatException e) {
            return fallback;
        }
    }

    /** Writes one record to the effect store after {@code args} ticks. */
    public static final Handler EFFECT = new Handler() {
        @Override
        public int durationTicks(String args) {
            return ticks(args, 5);
        }

        @Override
        public Result run(String runId, long token, String args, EffectStore store) {
            return switch (store.apply(runId, token)) {
                case APPLIED, ALREADY_APPLIED -> Result.SUCCESS;
                case STALE -> Result.STALE;
            };
        }
    };

    /** Always fails after one tick. Used to test retries. */
    public static final Handler FAIL = new Handler() {
        @Override
        public int durationTicks(String args) {
            return 1;
        }

        @Override
        public Result run(String runId, long token, String args, EffectStore store) {
            return Result.FAILURE;
        }
    };

    public static Map<String, Handler> builtIn() {
        Map<String, Handler> map = new HashMap<>();
        map.put("effect", EFFECT);
        map.put("fail", FAIL);
        return map;
    }
}
