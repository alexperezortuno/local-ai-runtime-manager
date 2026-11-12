package scheduler

type leaseQueue []string

func (q *leaseQueue) push(id string) { *q = append(*q, id) }

func (q *leaseQueue) pop() (string, bool) {
	if len(*q) == 0 {
		return "", false
	}
	id := (*q)[0]
	copy(*q, (*q)[1:])
	*q = (*q)[:len(*q)-1]
	return id, true
}

func (q *leaseQueue) remove(id string) bool {
	for i, queued := range *q {
		if queued == id {
			copy((*q)[i:], (*q)[i+1:])
			*q = (*q)[:len(*q)-1]
			return true
		}
	}
	return false
}
