package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/smtp"
	"os"
	"os/signal"
	"strings"
	"sync"

	"github.com/go-redis/redis/v8"
)

var ctx = context.Background()

type Task struct {
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

func main() {
	options, err := validateEnvironment()
	if err != nil {
		fmt.Println(err)
		return
	}
	printDetails(options)

	mailAuth := smtp.PlainAuth("", options.SMTPUsername, options.SMTPPassword, options.SMTPHost)

	rdb := redis.NewClient(&redis.Options{
		Addr: options.RedisAddress,
	})
	defer rdb.Close()

	wg := sync.WaitGroup{}

	go func() {
		for {
			res, err := rdb.BRPop(ctx, 0, "tasks").Result()
			if err != nil {
				fmt.Println("pop", err)
			}
			fmt.Println("Task received. Processing...")
			wg.Add(1)
			taskBody := res[1]
			task := Task{}
			err = json.Unmarshal([]byte(taskBody), &task)
			if err != nil {
				fmt.Println("unmarshal", err)
				return
			}
			go sendMail(&wg, options, mailAuth, task)
		}
	}()

	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, os.Interrupt)
	fmt.Printf("Listening for tasks on list '%s' at %s\n", options.RedisKey, options.RedisAddress)
	<-sigchan
	fmt.Println("Waiting for in-progress tasks to finish...")
	wg.Wait()
	fmt.Println("Tasks finished. Exiting...")
}

func printDetails(options AppOptions) {
	fmt.Printf("\n=========\n"+"Post Room\n"+"=========\n"+"Redis Server:\t%s\n"+"Redis List:\t%s\n"+"Mail Server:\t%s:%s\n\n", options.RedisAddress, options.RedisKey, options.SMTPHost, options.SMTPPort)
}

func sendMail(wg *sync.WaitGroup, options AppOptions, auth smtp.Auth, mail Task) {
	defer wg.Done()
	const messageTemplate = "Content-Type: text/html; charset=\"UTF-8\";\r\n" +
		"To: %s\r\n" +
		"From: %s\r\n" +
		"Subject: %s\r\n\r\n%s"
	message := []byte(fmt.Sprintf(messageTemplate, strings.Join(mail.Recipients, ", "), options.SenderAddress, mail.Subject, mail.Message))

	fmt.Printf("Sending email to SMTP server...\n")
	err := smtp.SendMail(fmt.Sprintf("%s:%s", options.SMTPHost, options.SMTPPort),
		auth,
		options.SenderAddress,
		mail.Recipients,
		message,
	)

	if err != nil {
		fmt.Println("SMTP ERROR", err)
		return
	}
	fmt.Println("Email sent successfully")

}

func validateEnvironment() (AppOptions, error) {
	const errorTemplate = "no ENV value provided for %s"
	options := AppOptions{}
	username, ok := os.LookupEnv(smtpUsernameKey)
	if !ok {
		return options, fmt.Errorf(errorTemplate, smtpUsernameKey)
	}
	options.SMTPUsername = username

	password, ok := os.LookupEnv(smtpPasswordKey)
	if !ok {
		return options, fmt.Errorf(errorTemplate, smtpPasswordKey)
	}
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
