package main

import (
	"context"
	"database/sql"
	"fmt"

	pb "greateapot.re/dblabs-api"
)

type ApiServer struct {
	pb.UnimplementedApiServer

	DB *sql.DB
}

func (s *ApiServer) execQuery(ctx context.Context, query string) error {
	tx, err := s.DB.Begin()
	if err != nil {
		return fmt.Errorf("failed begin tx, err: %s", err.Error())
	}
	defer tx.Rollback()

	if _, err = tx.ExecContext(ctx, query); err != nil {
		return fmt.Errorf("failed exec, err: %s; query: %s", err.Error(), query)
	} else if err = tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit changes, err: %s", err.Error())
	} else {
		return nil
	}
}

func (s *ApiServer) AlterDatabase(ctx context.Context, request *pb.AlterDatabaseRequest) (*pb.OkResponse, error) {
	if query, err := alterDatabaseQueryBuilder(request); err != nil {
		return &pb.OkResponse{
			Ok:    false,
			Error: &pb.ResponseError{Code: 0x000000A1, Message: err.Error()},
		}, nil
	} else if err := s.execQuery(ctx, query); err != nil {
		return &pb.OkResponse{
			Ok:    false,
			Error: &pb.ResponseError{Code: 0x000000A2, Message: err.Error()},
		}, nil
	} else {
		return &pb.OkResponse{
			Ok: true,
		}, nil
	}
}
func (s *ApiServer) AlterTable(ctx context.Context, request *pb.AlterTableRequest) (*pb.OkResponse, error) {
	if query, err := alterTableQueryBuilder(request); err != nil {
		return &pb.OkResponse{
			Ok:    false,
			Error: &pb.ResponseError{Code: 0x000000A1, Message: err.Error()},
		}, nil
	} else if err := s.execQuery(ctx, query); err != nil {
		return &pb.OkResponse{
			Ok:    false,
			Error: &pb.ResponseError{Code: 0x000000A2, Message: err.Error()},
		}, nil
	} else {
		return &pb.OkResponse{
			Ok: true,
		}, nil
	}
}
func (s *ApiServer) CreateDatabase(ctx context.Context, request *pb.CreateDatabaseRequest) (*pb.OkResponse, error) {
	if query, err := createDatabaseQueryBuilder(request); err != nil {
		return &pb.OkResponse{
			Ok:    false,
			Error: &pb.ResponseError{Code: 0x000000A1, Message: err.Error()},
		}, nil
	} else if err := s.execQuery(ctx, query); err != nil {
		return &pb.OkResponse{
			Ok:    false,
			Error: &pb.ResponseError{Code: 0x000000A2, Message: err.Error()},
		}, nil
	} else {
		return &pb.OkResponse{
			Ok: true,
		}, nil
	}
}
func (s *ApiServer) CreateTable(ctx context.Context, request *pb.CreateTableRequest) (*pb.OkResponse, error) {
	if query, err := createTableQueryBuilder(request); err != nil {
		return &pb.OkResponse{
			Ok:    false,
			Error: &pb.ResponseError{Code: 0x000000A1, Message: err.Error()},
		}, nil
	} else if err := s.execQuery(ctx, query); err != nil {
		return &pb.OkResponse{
			Ok:    false,
			Error: &pb.ResponseError{Code: 0x000000A2, Message: err.Error()},
		}, nil
	} else {
		return &pb.OkResponse{
			Ok: true,
		}, nil
	}
}
func (s *ApiServer) DropDatabase(ctx context.Context, request *pb.DropDatabaseRequest) (*pb.OkResponse, error) {
	if query, err := dropDatabaseQueryBuilder(request); err != nil {
		return &pb.OkResponse{
			Ok:    false,
			Error: &pb.ResponseError{Code: 0x000000A1, Message: err.Error()},
		}, nil
	} else if err := s.execQuery(ctx, query); err != nil {
		return &pb.OkResponse{
			Ok:    false,
			Error: &pb.ResponseError{Code: 0x000000A2, Message: err.Error()},
		}, nil
	} else {
		return &pb.OkResponse{
			Ok: true,
		}, nil
	}
}
func (s *ApiServer) DropTable(ctx context.Context, request *pb.DropTableRequest) (*pb.OkResponse, error) {
	if query, err := dropTableQueryBuilder(request); err != nil {
		return &pb.OkResponse{
			Ok:    false,
			Error: &pb.ResponseError{Code: 0x000000A1, Message: err.Error()},
		}, nil
	} else if err := s.execQuery(ctx, query); err != nil {
		return &pb.OkResponse{
			Ok:    false,
			Error: &pb.ResponseError{Code: 0x000000A2, Message: err.Error()},
		}, nil
	} else {
		return &pb.OkResponse{
			Ok: true,
		}, nil
	}
}
func (s *ApiServer) RenameTable(ctx context.Context, request *pb.RenameTableRequest) (*pb.OkResponse, error) {
	if query, err := renameTableQueryBuilder(request); err != nil {
		return &pb.OkResponse{
			Ok:    false,
			Error: &pb.ResponseError{Code: 0x000000A1, Message: err.Error()},
		}, nil
	} else if err := s.execQuery(ctx, query); err != nil {
		return &pb.OkResponse{
			Ok:    false,
			Error: &pb.ResponseError{Code: 0x000000A2, Message: err.Error()},
		}, nil
	} else {
		return &pb.OkResponse{
			Ok: true,
		}, nil
	}
}
func (s *ApiServer) TruncateTable(ctx context.Context, request *pb.TruncateTableRequest) (*pb.OkResponse, error) {
	if query, err := truncateTableQueryBuilder(request); err != nil {
		return &pb.OkResponse{
			Ok:    false,
			Error: &pb.ResponseError{Code: 0x000000A1, Message: err.Error()},
		}, nil
	} else if err := s.execQuery(ctx, query); err != nil {
		return &pb.OkResponse{
			Ok:    false,
			Error: &pb.ResponseError{Code: 0x000000A2, Message: err.Error()},
		}, nil
	} else {
		return &pb.OkResponse{
			Ok: true,
		}, nil
	}
}
