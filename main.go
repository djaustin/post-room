package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/smtp"
	"os"
	"os/signal"
	"strings"
	"sync"

	"github.com/go-redis/redis/v8"
)

var ctx = context.Background()

type Mail struct {
	Subject    string   `json:"subject"`
	Message    string   `json:"message"`
	Recipients []string `json:"recipients"`
}

type AppOptions struct {
	SMTPUsername, SMTPPassword, SMTPHost, SMTPPort, SenderAddress, RedisAddress, RedisKey string
}

const (
	smtpUsernameKey  = "SMTP_USERNAME"
	smtpPasswordKey  = "SMTP_PASSWORD"
	smtpHostKey      = "SMTP_HOST"
	smtpPortKey      = "SMTP_PORT"
	senderAddressKey = "SENDER_ADDRESS"
	redisAddressKey  = "REDIS_ADDRESS"
	redisKeyKey      = "REDIS_KEY"
)

type Mailer struct {
	template, senderAddress, host, port string
	auth                                smtp.Auth
}

func (m Mailer) sendMail(mail Mail) {
	mail.Message = fmt.Sprintf(m.template, strings.Join(mail.Recipients, ", "), m.senderAddress, mail.Subject, mail.Message)
	if m.auth == nil {
		err := m.sendMailUnauthenticated(mail)
		if err != nil {
			log.Printf("error sending without authentication: %v", err)
		}
		return
	}
	log.Printf("sending email to SMTP server...\n")
	err := smtp.SendMail(fmt.Sprintf("%s:%s", m.host, m.port),
		m.auth,
		m.senderAddress,
		mail.Recipients,
		[]byte(mail.Message),
	)

	if err != nil {
		log.Print("error sending email to server: ", err)
		return
	}
	log.Print("email sent successfully")
}

func (m Mailer) sendMailUnauthenticated(mail Mail) error {
	// Connect to the remote SMTP server.
	c, err := smtp.Dial(fmt.Sprintf("%s:%s", m.host, m.port))
	if err != nil {
		return fmt.Errorf("error connecting to remote SMTP host: %w", err)
	}
	defer c.Quit()

	// Set the sender and recipient first
	if err := c.Mail(m.senderAddress); err != nil {
		return fmt.Errorf("error setting sender address: %w", err)
	}
	if err := c.Rcpt(mail.Recipients[0]); err != nil {
		return fmt.Errorf("error setting recipient address: %w", err)
	}

	// Send the email body.
	wc, err := c.Data()
	if err != nil {
		return fmt.Errorf("error issuing DATA command to remote SMTP host: %w", err)
	}

	_, err = fmt.Fprintf(wc, mail.Message)
	if err != nil {
		return fmt.Errorf("error writing message body: %w", err)
	}
	err = wc.Close()
	if err != nil {
		return fmt.Errorf("error closing message body writer: %w", err)
	}
	return nil
}

func main() {
	options, err := validateEnvironment()
	if err != nil {
		log.Println(err)
		return
	}
	printDetails(options)

	mailer := Mailer{
		template: "Content-Type: text/html; charset=\"UTF-8\";\r\n" +
			"To: %s\r\n" +
			"From: %s\r\n" +
			"Subject: %s\r\n\r\n%s",
		senderAddress: options.SenderAddress,
		host:          options.SMTPHost,
		port:          options.SMTPPort,
	}

	if len(options.SMTPUsername) > 0 && len(options.SMTPPassword) > 0 {
		mailer.auth = smtp.PlainAuth("", options.SMTPUsername, options.SMTPPassword, options.SMTPHost)
	} else {
		log.Println("[WARNING] No auth details provided, using unauthenticated SMTP")
	}

	rdb := redis.NewClient(&redis.Options{
		Addr: options.RedisAddress,
	})

	wg := sync.WaitGroup{}

	go func() {
		for {
			res, err := rdb.BRPop(ctx, 0, "tasks").Result()
			if err != nil {
				log.Fatalln("cannot pop from list:", err)
			}
			log.Print("processing task from list...")
			taskBody := res[1]
			task := Mail{}
			err = json.Unmarshal([]byte(taskBody), &task)
			if err != nil {
				log.Print("error unmarshalling task data to JSON: ", err)
				continue
			}
			wg.Add(1)
			go func() {
				mailer.sendMail(task)
				wg.Done()
			}()
		}
	}()

	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, os.Interrupt)
	log.Printf("worker registered for tasks on list '%s' at %s\n", options.RedisKey, options.RedisAddress)
	<-sigchan
	log.Print("waiting for in-progress tasks to finish...")
	wg.Wait()
	log.Println("tasks finished")
	log.Println("exiting...")
}

func printDetails(options AppOptions) {
	fmt.Printf("\n=========\n"+"Post Room\n"+"=========\n"+"Redis Server:\t%s\n"+"Redis List:\t%s\n"+"Mail Server:\t%s:%s\n\n", options.RedisAddress, options.RedisKey, options.SMTPHost, options.SMTPPort)
}

func validateEnvironment() (AppOptions, error) {
	const errorTemplate = "no ENV value provided for %s"
	options := AppOptions{}
	username, _ := os.LookupEnv(smtpUsernameKey)
	options.SMTPUsername = username

	password, _ := os.LookupEnv(smtpPasswordKey)
	options.SMTPPassword = password

	host, ok := os.LookupEnv(smtpHostKey)
	if !ok {
		return options, fmt.Errorf(errorTemplate, smtpHostKey)
	}
	options.SMTPHost = host

	port, ok := os.LookupEnv(smtpPortKey)
	if !ok {
		return options, fmt.Errorf(errorTemplate, smtpPortKey)
	}
	options.SMTPPort = port

	address, ok := os.LookupEnv(senderAddressKey)
	if !ok {
		return options, fmt.Errorf(errorTemplate, senderAddressKey)
	}
	options.SenderAddress = address

	redisAddress, ok := os.LookupEnv(redisAddressKey)
	if !ok {
		return options, fmt.Errorf(errorTemplate, redisAddressKey)
	}
	options.RedisAddress = redisAddress

	redisKey, ok := os.LookupEnv(redisKeyKey)
	if !ok {
		options.RedisKey = "tasks"
	} else {

		options.RedisKey = redisKey
	}
	return options, nil
}
