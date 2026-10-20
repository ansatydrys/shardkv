// Package protocol is a small hand-written binding for api.proto.
// Using the well-known Struct message avoids generated application message code.
package protocol

import (
	"context"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/structpb"
)

type Message = structpb.Struct

func MessageOf(v map[string]any) *Message {
	m, err := structpb.NewStruct(v)
	if err != nil {
		panic(err)
	}
	return m
}

type Server interface {
	Call(context.Context, string, *Message) (*Message, error)
}

func Register(g *grpc.Server, s Server) {
	methods := []grpc.MethodDesc{}
	for _, method := range []string{"Put", "Get", "Delete", "Replicate", "Status"} {
		name := method
		methods = append(methods, grpc.MethodDesc{MethodName: name, Handler: func(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
			in := new(Message)
			if err := dec(in); err != nil {
				return nil, err
			}
			handler := func(ctx context.Context, req any) (any, error) { return srv.(Server).Call(ctx, name, req.(*Message)) }
			if interceptor == nil {
				return handler(ctx, in)
			}
			return interceptor(ctx, in, &grpc.UnaryServerInfo{Server: srv, FullMethod: "/shardkv.KV/" + name}, handler)
		}})
	}
	g.RegisterService(&grpc.ServiceDesc{ServiceName: "shardkv.KV", HandlerType: (*Server)(nil), Methods: methods}, s)
}
func Call(ctx context.Context, c grpc.ClientConnInterface, method string, in *Message) (*Message, error) {
	out := new(Message)
	err := c.Invoke(ctx, "/shardkv.KV/"+method, in, out)
	return out, err
}

// CallReady waits for reconnect within the caller deadline (for replica catch-up).
func CallReady(ctx context.Context, c grpc.ClientConnInterface, method string, in *Message) (*Message, error) {
	out := new(Message)
	err := c.Invoke(ctx, "/shardkv.KV/"+method, in, out, grpc.WaitForReady(true))
	return out, err
}
