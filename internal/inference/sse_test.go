package inference

import "testing"

// TestSSEParserDetectsChatCompletionsDone keeps the Chat Completions
// terminator behavior unchanged.
func TestSSEParserDetectsChatCompletionsDone(t *testing.T) {
	var parser SSEParser
	if parser.Feed([]byte("data: {\"choices\":[{\"delta\":{\"content\":\"one\"}}]}\n\n")) {
		t.Fatal("a content chunk was reported as the terminator")
	}
	if !parser.Feed([]byte("data: [DONE]\n\n")) {
		t.Fatal("the [DONE] sentinel was not detected")
	}
}

// TestSSEParserDetectsResponsesCompleted proves the Responses API terminal
// event is detected. A Responses stream carries no "[DONE]" sentinel.
func TestSSEParserDetectsResponsesCompleted(t *testing.T) {
	var parser SSEParser
	stream := "event: response.created\ndata: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_1\"}}\n\n" +
		"event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"item_id\":\"msg_1\",\"delta\":\"one\"}\n\n"
	if parser.Feed([]byte(stream)) {
		t.Fatal("a content delta was reported as the terminator")
	}
	if !parser.Feed([]byte("event: response.completed\ndata: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_1\"}}\n\n")) {
		t.Fatal("the response.completed event was not detected")
	}
	if !parser.Finish() {
		t.Fatal("the terminator was lost at the end of the stream")
	}
}

// TestSSEParserResponsesStreamWithoutTerminalEvent proves a Responses stream
// cut before "response.completed" is not reported as complete.
func TestSSEParserResponsesStreamWithoutTerminalEvent(t *testing.T) {
	var parser SSEParser
	stream := "event: response.created\ndata: {\"type\":\"response.created\"}\n\n" +
		"event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"delta\":\"one\"}\n\n"
	parser.Feed([]byte(stream))
	if parser.Finish() {
		t.Fatal("a truncated Responses stream was reported as complete")
	}
}

// TestSSEParserIgnoresOtherEventNames proves only the terminal event ends the
// stream.
func TestSSEParserIgnoresOtherEventNames(t *testing.T) {
	var parser SSEParser
	stream := "event: response.in_progress\ndata: {\"type\":\"response.in_progress\"}\n\n" +
		"event: response.output_item.done\ndata: {\"type\":\"response.output_item.done\"}\n\n"
	parser.Feed([]byte(stream))
	if parser.Finish() {
		t.Fatal("a non-terminal event ended the stream")
	}
}
