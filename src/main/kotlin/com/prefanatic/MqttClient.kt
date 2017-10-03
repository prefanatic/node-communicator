package com.prefanatic

import io.reactivex.Observable
import io.reactivex.Single
import io.reactivex.SingleEmitter
import io.reactivex.subjects.PublishSubject
import org.eclipse.paho.client.mqttv3.*

class RxMqttClient(serverURI: String?,
                   clientId: String?,
                   persistence: MqttClientPersistence? = null) {

    private val client = MqttAsyncClient(serverURI, clientId, persistence)
    private val messageMap = HashMap<String, PublishSubject<Message>>()

    fun connect(): Single<IMqttToken> = Single.create<IMqttToken> {
        val context = Any()
        val options = MqttConnectOptions()

        client.connect(options, context, RxMqttActionListener(it))
    }

    fun subscribeAndObserve(topic: String, qos: Int): Observable<Message> {
        return subscribe(topic, qos)
                .flatMapObservable { observeMessages(topic) }
    }

    fun subscribe(topic: String, qos: Int): Single<IMqttToken> = Single.create {
        val context = Any()

        if (messageMap.containsKey(topic)) throw RuntimeException("Already subscribed to topic: $topic")
        messageMap.put(topic, PublishSubject.create())

        client.subscribe(topic, qos, context, RxMqttActionListener(it), this::onMessageArrivedInternal)
    }

    /**
     * Publishes an MQTT message.
     * Returns a Single that will emit a [IMqttToken] when the publish succeeds.
     * @param topic MQTT Topic
     * @param payload Payload
     * @param qos QoS
     * @param retained Retained
     * @return Single of [IMqttToken]
     */
    fun publish(topic: String, payload: ByteArray, qos: Int, retained: Boolean): Single<IMqttToken> = Single.create {
        val context = Any()

        client.publish(topic, payload, qos, retained, context, RxMqttActionListener(it))
    }

    fun observeMessages(topic: String): Observable<Message> = messageMap[topic]
            ?: throw RuntimeException("A subscribe must be issued before observing topic: $topic")

    private fun onMessageArrivedInternal(topic: String, message: MqttMessage) {
        val subject = messageMap[topic]
                ?: throw RuntimeException("Received message for topic: $topic -- but we don't subscribe to this!")

        subject.onNext(Message(topic, message))
    }
}

data class Message(val topic: String, val message: MqttMessage)

private class RxMqttActionListener<out T : SingleEmitter<IMqttToken>>(val emitter: T) : IMqttActionListener {
    override fun onSuccess(asyncActionToken: IMqttToken) {
        emitter.onSuccess(asyncActionToken)
    }

    override fun onFailure(asyncActionToken: IMqttToken, exception: Throwable) {
        emitter.onError(exception)
    }
}

private fun <T> rxActionCallback(emitter: SingleEmitter<IMqttToken>): IMqttActionListener = object : IMqttActionListener {
    override fun onSuccess(asyncActionToken: IMqttToken) {
        emitter.onSuccess(asyncActionToken)
    }

    override fun onFailure(asyncActionToken: IMqttToken, exception: Throwable) {
        emitter.onError(exception)
    }
}