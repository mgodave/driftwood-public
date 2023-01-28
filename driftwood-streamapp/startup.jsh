import java.util.*;
import java.util.concurrent.*;
import org.robotninjas.stream.*;
import org.robotninjas.stream.client.*;
import org.robotninjas.app.stream.commands.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

println("""
   oooooooooo.   ooooooooo.   ooooo oooooooooooo ooooooooooooo
   `888'   `Y8b  `888   `Y88. `888' `888'     `8 8'   888   `8
    888      888  888   .d88'  888   888              888
    888      888  888ooo88P'   888   888oooo8         888
    888      888  888`88b.     888   888    "         888
    888     d88'  888  `88b.   888   888              888
   o888bood8P'   o888o  o888o o888o o888o            o888o

oooooo   oooooo     oooo   .oooooo.     .oooooo.   oooooooooo.
 `888.    `888.     .8'   d8P'  `Y8b   d8P'  `Y8b  `888'   `Y8b
  `888.   .8888.   .8'   888      888 888      888  888      888
   `888  .8'`888. .8'    888      888 888      888  888      888
    `888.8'  `888.8'     888      888 888      888  888      888
     `888'    `888'      `88b    d88' `88b    d88'  888     d88'
      `8'      `8'        `Y8bood8P'   `Y8bood8P'  o888bood8P'
""")

var executor = Executors.newCachedThreadPool()

Future<?> run_command_async(Callable<?> cmd) {
    return executor.submit(cmd);
}

Object run_command_sync(Callable<?> cmd) throws Exception {
    return run_command_async(cmd).get();
}