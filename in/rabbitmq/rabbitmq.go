package rabbitmq

import (
	"time"
	"flag"
	"strconv"
	"github.com/raintank/met"
	"github.com/raintank/metrictank/idx"
	"github.com/raintank/metrictank/in"
	"github.com/raintank/metrictank/mdata"
	"github.com/raintank/metrictank/usage"
	"github.com/raintank/worldping-api/pkg/log"
	"github.com/rakyll/globalconf"
	"github.com/streadway/amqp"
)

type connState struct {
	conn    *amqp.Connection
	ch      *amqp.Channel
	q       amqp.Queue
	closing chan *amqp.Error
}

type RabbitMQ struct {
	in.In
	addr string
	port    int
	queue   string
	user    string
	pass    string
	vhost   string
	url     string
	stats   met.Backend
	state   connState
}

var Enabled bool
var addr string
var port int
var queue string
var user string
var pass string
var vhost string

func ConfigSetup() {
	inRabbitMQ := flag.NewFlagSet("rabbitmq-in", flag.ExitOnError)
	inRabbitMQ.BoolVar(&Enabled, "enabled", false, "")
	inRabbitMQ.StringVar(&addr, "addr", "127.0.0.1", "RabbitMQ address")
	inRabbitMQ.IntVar(&port, "port", 5672, "RabbitMQ port")
	inRabbitMQ.StringVar(&vhost, "vhost", "/", "RabbitMQ virtual host")
	inRabbitMQ.StringVar(&queue, "queue", "defaultq", "RabbitMQ Queue name")
	inRabbitMQ.StringVar(&user, "user", "guest", "RabbitMQ User")
	inRabbitMQ.StringVar(&pass, "pass", "guest", "RabbitMQ Password")
	globalconf.Register("rabbitmq-in", inRabbitMQ)
}

func New(stats met.Backend) *RabbitMQ {
	return &RabbitMQ{
		addr:    addr,
		port:    port,
		queue:   queue,
		user:    user,
		pass:    pass,
		vhost:   vhost,
		stats:   stats,
	}
}

func (r *RabbitMQ) connect() (bool) {
	log.Info("Connecting to Rabbit")
	conn, err := amqp.Dial(r.url)
	if (err != nil) {
		log.Critical(4, err.Error())
		return false
	}
	r.state.conn = conn
	closeNotification := make(chan *amqp.Error)
	r.state.closing = closeNotification
	conn.NotifyClose(closeNotification)

	ch, err := conn.Channel()
	if (err != nil) {
		log.Critical(4, err.Error())
		return false
	}
	r.state.ch = ch
	ch.NotifyClose(closeNotification)

	q, err := ch.QueueDeclare(
		r.queue, // queue name
		false,   // durable
		true,    // auto delete
		false,   // exclusive
		false,   // no wait
		nil,     // args
	)
	if (err != nil) {
		log.Critical(4, err.Error())
	}
	r.state.q = q

	return true
}

func (r *RabbitMQ) connectLoop() {
	waitTime := time.Second * 3
	for (!r.connect()) {
		log.Info("Connect failed, retrying...")
		time.Sleep(waitTime)
	}
	log.Info("Connected")
}

func (r *RabbitMQ) disconnect() {
	if (r.state.conn != nil) {
		r.state.conn.Close() // close channels and connection
	}
}

func (r *RabbitMQ) consume() (<-chan amqp.Delivery, error) {
	msgs, err := r.state.ch.Consume(
		r.queue, // queue
		"",      // consumer string
		true,    // auto ack
		false,   // exclusive
		false,   // no local
		false,   // no wait
		nil,     // args
	)
	return msgs, err
}

func (r *RabbitMQ) reconnect() {
	log.Info("Reconnecting to Rabbit")
	r.connectLoop()
}

func (r *RabbitMQ) process(d amqp.Delivery) {
	r.In.Handle(d.Body)
}

func (r *RabbitMQ) consumeLoop() {
	r.connectLoop()
	for {
		msgs, err := r.consume()
		if (err != nil) {
			log.Warn(err.Error())
			r.reconnect()
			continue
		}

		go func() {
			for d := range msgs {
				r.process(d)
			}
		}()
		<-r.state.closing
		log.Info("Rabbit closed the connection")
		r.reconnect()
	}
}

func (r *RabbitMQ) Start(metrics mdata.Metrics, metricIndex idx.MetricIndex, usg *usage.Usage) {
	r.In = in.New(metrics, metricIndex, usg, "rabbitMq", r.stats)
	r.url = "amqp://" + r.user + ":" + r.pass + "@" + r.addr + ":" + strconv.Itoa(r.port) + r.vhost

	go r.consumeLoop()
}
