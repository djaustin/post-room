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
	smtpUsername, smtpPassword, smtpHost, smtpPort, senderAddress string
}

const (
	smtpUsernameKey  = "SMTP_USERNAME"
	smtpPasswordKey  = "SMTP_PASSWORD"
	smtpHostKey      = "SMTP_HOST"
	smtpPortKey      = "SMTP_PORT"
	senderAddressKey = "SENDER_ADDRESS"
)

func main() {
	options, err := validateEnvironment()
	if err != nil {
		fmt.Println(err)
		return
	}

	mailAuth := smtp.PlainAuth("", options.smtpUsername, options.smtpPassword, options.smtpHost)

	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	defer rdb.Close()

	wg := sync.WaitGroup{}

	go func() {
		for {
			res, err := rdb.BRPop(ctx, 0, "tasks").Result()
			if err != nil {
				fmt.Println("pop", err)
			}
			fmt.Println("Handling task...")
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
	<-sigchan
	fmt.Println("Waiting for in-progress tasks to finish...")
	wg.Wait()
	fmt.Println("Tasks finished. Exiting...")
}

func sendMail(wg *sync.WaitGroup, options AppOptions, auth smtp.Auth, mail Task) {
	const messageTemplate = "Content-Type: text/html; charset=\"UTF-8\";\r\n" +
		"To: %s\r\n" +
		"From: %s\r\n" +
		"Subject: %s\r\n\r\n%s"
	fmt.Println("Sending email...")
	message := []byte(fmt.Sprintf(messageTemplate, strings.Join(mail.Recipients, ", "), options.senderAddress, mail.Subject, mail.Message))

	err := smtp.SendMail(fmt.Sprintf("%s:%s", options.smtpHost, options.smtpPort),
		auth,
		options.senderAddress,
		mail.Recipients,
		message,
	)

	if err != nil {
		fmt.Println("SMTP ERROR", err)
		wg.Done()
		return
	}
	fmt.Println("Sent!")
	wg.Done()

}

func validateEnvironment() (AppOptions, error) {
	options := AppOptions{}
	username, ok := os.LookupEnv(smtpUsernameKey)
	if !ok {
		return options, fmt.Errorf("no ENV value provided for %s", smtpUsernameKey)
	}
	options.smtpUsername = username

	password, ok := os.LookupEnv(smtpPasswordKey)
	if !ok {
		return options, fmt.Errorf("no ENV value provided for %s", smtpPasswordKey)
	}
	options.smtpPassword = password

	host, ok := os.LookupEnv(smtpHostKey)
	if !ok {
		return options, fmt.Errorf("no ENV value provided for %s", smtpHostKey)
	}
	options.smtpHost = host

	port, ok := os.LookupEnv(smtpPortKey)
	if !ok {
		return options, fmt.Errorf("no ENV value provided for %s", smtpPortKey)
	}
	options.smtpPort = port

	address, ok := os.LookupEnv(senderAddressKey)
	if !ok {
		return options, fmt.Errorf("no ENV value provided for %s", senderAddressKey)
	}
	options.senderAddress = address
	return options, nil
}
