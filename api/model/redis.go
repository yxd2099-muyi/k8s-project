package model

type UserSession struct {
	UserID      uint64 `redis:"user_id"`      // 用户ID
	GateAddress string `redis:"gate_address"` // 所在gate address
}
