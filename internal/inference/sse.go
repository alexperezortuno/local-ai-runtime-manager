package inference

import "strings"

// SSEParser retains only the current line and a tiny DONE candidate; it never
// buffers prompts, completion chunks, or a complete response.
//
// It recognizes the two stream terminators of the OpenAI-compatible surface:
// the Chat Completions sentinel "data: [DONE]" and the Responses API event
// "event: response.completed", which is the terminal event of the Responses
// stream. A Responses stream has no "[DONE]" sentinel.
type SSEParser struct {
	line       []byte
	dataLines  uint8
	done       bool
	candidate  bool
	count      uint64
	discarding bool
}

func (p *SSEParser) Feed(chunk []byte) bool {
	if len(chunk) > 0 {
		p.count++
	}
	for _, b := range chunk {
		if b == '\n' {
			p.lineEvent()
			p.line = p.line[:0]
			p.discarding = false
			continue
		}
		if p.discarding {
			continue
		}
		if len(p.line) >= 4096 {
			p.discarding = true
			continue
		}
		p.line = append(p.line, b)
	}
	return p.done
}

func (p *SSEParser) Count() uint64 { return p.count }

func (p *SSEParser) Finish() bool {
	if len(p.line) > 0 {
		p.lineEvent()
		p.line = p.line[:0]
	}
	p.dispatch()
	return p.done
}

func (p *SSEParser) lineEvent() {
	line := strings.TrimSuffix(string(p.line), "\r")
	if line == "" {
		p.dispatch()
		return
	}
	if strings.HasPrefix(line, "event:") {
		// The Responses API names every event; its terminal event is
		// "response.completed". Chat Completions streams have no event name.
		name := strings.TrimSpace(strings.TrimPrefix(line, "event:"))
		if name == "response.completed" {
			p.done = true
		}
		return
	}
	if !strings.HasPrefix(line, "data:") {
		return
	}
	value := strings.TrimPrefix(line, "data:")
	value = strings.TrimPrefix(value, " ")
	p.dataLines++
	if p.dataLines == 1 && value == "[DONE]" {
		p.candidate = true
		return
	}
	p.candidate = false
}

func (p *SSEParser) dispatch() {
	if p.dataLines == 1 && p.candidate {
		p.done = true
	}
	p.dataLines = 0
	p.candidate = false
}
