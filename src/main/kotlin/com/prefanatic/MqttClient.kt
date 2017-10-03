package com.prefanatic

import io.reactivex.Completable
import io.reactivex.Observable
import io.reactivex.Single
import io.reactivex.SingleEmitter
import org.eclipse.paho.client.mqttv3.*

class RxMqttClient(serverURI: String?,
                   clientId: String?,
                   persistence: MqttClientPersistence? = null) {

    private val client = MqttAsyncClient(serverURI, clientId, persistence)

    fun connect(): Single<IMqttToken> = Single.create<IMqttToken> {
        val context = Any()
        val options = MqttConnectOptions()

        client.connect(options, context, RxMqttActionListener(it))
    }

    fun subscribe(topic: String, qos: Int): Observable<Pair<String, MqttMessage>> = Observable.create {
        val context = Any()
        val listener = object : IMqttActionListener {
            override fun onSuccess(asyncActionToken: IMqttToken) {
                //emitter.onNext(asyncActionToken)
                val tada = true
            }

            override fun onFailure(asyncActionToken: IMqttToken, exception: Throwable) {
                it.onError(exception)
            }
        }

        client.subscribe(topic, qos, context, listener) { topic, message ->
            it.onNext(Pair(topic, message))
        }
    }

    fun publish(topic: String, payload: ByteArray, qos: Int, retained: Boolean): Single<IMqttToken> = Single.create {
        val context = Any()
        val listener = object : IMqttActionListener {
            override fun onSuccess(asyncActionToken: IMqttToken) {
                it.onSuccess(asyncActionToken)
            }

            override fun onFailure(asyncActionToken: IMqttToken, exception: Throwable) {
                it.onError(exception)
            }
        }


        client.publish(topic, payload, qos, retained, context, listener)
    }
}

private class RxMqttActionListener<out T : SingleEmitter<IMqttToken>>(val emitter: T) : IMqttActionListener {
    override fun onSuccess(asyncActionToken: IMqttToken) {
        emitter.onSuccess(asyncActionToken)
    }

    override fun onFailure(asyncActionToken: IMqttToken, exception: Throwable) {
        emitter.onError(exception)
    }
}

fun MqttClient.rxConnect(): Single<Any> = Single.create { emitter ->
    connect()
    emitter.onSuccess(Any())
}

fun MqttClient.rxSubscribe(topicFilter: String, qos: Int): Observable<Message> = Observable.create { emitter ->
    subscribe(topicFilter, qos) { topic, message ->
        emitter.onNext(Message(topic, message))
    }
}

fun MqttClient.rxPublish(topic: String, payload: ByteArray, qos: Int, retained: Boolean): Completable = Completable.create {
    publish(topic, payload, qos, retained)
    it.onComplete()
}

private fun <T> rxActionCallback(emitter: SingleEmitter<IMqttToken>): IMqttActionListener = object : IMqttActionListener {
    override fun onSuccess(asyncActionToken: IMqttToken) {
        emitter.onSuccess(asyncActionToken)
    }

    override fun onFailure(asyncActionToken: IMqttToken, exception: Throwable) {
        emitter.onError(exception)
    }
}

data class Message(val topic: String, val message: MqttMessage)