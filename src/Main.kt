import ru.gluck1986.unique.ip.utils.UniqueCounter
import java.time.LocalDateTime

fun main(args: Array<String>) {
    println(LocalDateTime.now())
    val fileName = if (args.isNotEmpty()) args[0] else throw Exception("need file name in first argument")
    val counter = UniqueCounter(7)

    counter.setProgressHandler { total, current ->
        val procents = current * 100L / if (total == 0L) 1L else total
        print("$procents%\r")
    }
    counter.setOnFinish { rows, unique, errors ->
        println()
        println("total unique: $unique, total rows $rows")
        if (errors.isNotEmpty()) {
            println("errors:")
            errors.forEach {
                println(it)
            }
        }
    }

    counter.count(fileName)

    println(LocalDateTime.now())
}