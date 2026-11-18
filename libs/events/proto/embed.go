// Package protos embeds the .proto sources so the registry bootstrap registers
// exactly the text the generated code was built from.
package protos

import "embed"

// FS holds every .proto file at the same relative paths buf uses (for example
// "smartcourse/identity/v1/events.proto").
//
//go:embed smartcourse
var FS embed.FS
