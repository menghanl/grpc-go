module google.golang.org/grpc/security/advancedtls/examples

go 1.15

require (
	google.golang.org/grpc v1.33.1
	google.golang.org/grpc/examples v0.0.0-20201204235607-0d6a24f68a5f
	google.golang.org/grpc/security/advancedtls v0.0.0-20201112215255-90f1b3ee835b
)

replace google.golang.org/grpc => ../../..

replace google.golang.org/grpc/examples => ../../../examples

replace google.golang.org/grpc/security/advancedtls => ../
