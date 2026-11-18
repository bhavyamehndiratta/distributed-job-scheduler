package io.github.bhavyamehndiratta.djs.scheduler;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import org.junit.jupiter.api.Test;

class CommandCodecTest {

    private static final String AWKWARD = "a|b%c~d\ne\rf 100% | ~ \u00e9";

    @Test
    void everyCommandRoundTrips() {
        List<Command> commands = List.of(
                new Command.SubmitJob("job-1", JobSpec.once("echo", AWKWARD, 5_000, 3), "key-1", 123),
                new Command.SubmitJob("job-2", JobSpec.recurring("sleep", "", "*/5 * * * *", 1), null, 0),
                new Command.CancelJob("job-1"),
                new Command.FireRun("job-2", 300_000, 4),
                new Command.AssignRun("job-2@300000", "worker-7", 20),
                new Command.RunStarted("job-2@300000", 42),
                new Command.RunCompleted("job-2@300000", 42, true, ""),
                new Command.RunCompleted("job-2@300000", 42, false, AWKWARD),
                new Command.ExpireAssignment("job-2@300000", 42));
        for (Command c : commands) {
            String text = CommandCodec.encode(c);
            assertFalse(text.contains("\n"), "log entries must be one line: " + text);
            assertEquals(c, CommandCodec.decode(text));
        }
    }

    @Test
    void malformedTextIsRejectedWithIllegalArgument() {
        for (String bad : new String[] {"", "Z|x", "S|a", "F|job|notanumber|0", "F|job|1", "T|run|0",
                "D|run|5|2|x", "C|", "C|~", "A|run|w|0", "F|job|1|0|extra", "X|run|%4", "X|run|%zz"}) {
            assertThrows(IllegalArgumentException.class, () -> CommandCodec.decode(bad), bad);
        }
        assertThrows(IllegalArgumentException.class, () -> CommandCodec.decode(null));
    }

    @Test
    void invalidSpecsAreRejectedBeforeTheyReachTheLog() {
        assertThrows(IllegalArgumentException.class, () -> JobSpec.once("", "", 0, 1));
        assertThrows(IllegalArgumentException.class, () -> JobSpec.once("h", "", 0, 0));
        assertThrows(IllegalArgumentException.class, () -> JobSpec.recurring("h", "", "not cron", 1));
        assertThrows(IllegalArgumentException.class, () -> new Command.AssignRun("r", "w", 0));
    }
}
