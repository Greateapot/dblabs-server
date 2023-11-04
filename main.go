package main

import (
	"database/sql"
	"fmt"

	"log"
	"net"
	"time"

	"google.golang.org/grpc"

	_ "github.com/go-sql-driver/mysql"
	pb "greateapot.re/dblabs-api"
)

func main() {
	db, err := sql.Open(
		"mysql",
		SrvConf.DataSourceName(),
	)
	if err != nil {
		log.Panicf("failed to open db: %v", err)
	}

	defer db.Close()

	db.SetConnMaxLifetime(time.Minute * 3)
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(10)

	listener, err := net.Listen(
		SrvConf.ServerConnectionProtocol,
		fmt.Sprintf("%s:%d", SrvConf.ServerHost, SrvConf.ServerPort),
	)
	if err != nil {
		log.Panicf("failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterApiServer(grpcServer, &ApiServer{DB: db})
	grpcServer.Serve(listener)
}
