package com.prefanatic

import com.squareup.moshi.KotlinJsonAdapterFactory
import com.squareup.moshi.Moshi
import com.xenomachina.argparser.ArgParser
import io.reactivex.Observable
import io.reactivex.rxkotlin.subscribeBy
import io.reactivex.schedulers.Schedulers
import java.io.File
import java.util.*
import java.util.concurrent.TimeUnit

const val PERFORM_OTA = true

val moshi = Moshi.Builder()
        .add(KotlinJsonAdapterFactory())
        .build()

val nodeAdapter = moshi.adapter<Node>(Node::class.java)
val otaStartAdapter = moshi.adapter<OtaStart>(OtaStart::class.java)
val otaWriteAdapter = moshi.adapter<OtaWrite>(OtaWrite::class.java)

val client = RxMqttClient("tcp://misterman:1883", "communicator")

class MyArgs(parser: ArgParser) {

}

private fun heartbeat() = client.publish("/heartbeat", ByteArray(0), 0, false)

private fun performOta(node: Node) {
    val file = File("C:\\Users\\Cody Goldberg\\IdeaProjects\\NodeMCUSimulatorPrefan\\init.lua")
    val fileVersion = file.bufferedReader()
            .readLine()
            .replace("local INITVERSION = ", "")
            .toIntOrNull()
            ?: -1

    if (PERFORM_OTA && fileVersion > node.initVersion) {
        println("Sending OTA.")
        val _contents = file.bufferedReader().readLines()
        var i = -1

        val otaStart = otaStartAdapter.toJson(OtaStart("init.lua", _contents.size))
        client.publish("/ota/${node.id}", otaStart.toByteArray(), 2, false)
                .concatWith(
                        Observable.fromIterable(_contents)
                                .concatMap {
                                    val part = OtaWrite(i++, it)
                                    val contents = otaWriteAdapter.toJson(part).toByteArray()

                                    client.publish("/ota/${node.id}/part", contents, 2, false)
                                            .doOnSuccess { println("Sent $i -- ${part.content}") }
                                            .delay(100, TimeUnit.MILLISECONDS)
                                            .toObservable()
                                }
                                .lastOrError()
                )
                .lastOrError()
                .concatWith(client.publish("/ota/${node.id}/end", ByteArray(0), 2, false))
                .subscribeBy {
                    println("TADAH!")
                }
    }
}

fun main(args: Array<String>) {
    println("Hello World!")
    client.connect()
            .observeOn(Schedulers.newThread())
            .flatMap { heartbeat() }
            .flatMapObservable { client.subscribeAndObserve("/register", 0) }
            .subscribe({
                val node = nodeAdapter.fromJson(it.message.payload.toString(Charsets.UTF_8))!!
                println(node)
                performOta(node)

            }, { throwable -> throwable.printStackTrace() })

    val scanner = Scanner(System.`in`)

    try {
        while (true) {
            val command = scanner.nextLine()
            println("::$command")

            when (command) {
                "exit" -> System.exit(0)
                "heartbeat" -> heartbeat().subscribe()
            }
        }
    } catch (e: Exception) {
        e.printStackTrace()
    }
}

data class Node(val id: String, val description: String = "Unknown", val initVersion: Int = 0)

class OtaStart(val fileName: String, val parts: Int)
class OtaWrite(val part: Int, val content: String)