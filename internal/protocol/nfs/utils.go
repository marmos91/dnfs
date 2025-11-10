package nfs

// safeAdd performs checked addition of two uint64 values.
// Returns the sum and a boolean indicating whether overflow occurred.
func safeAdd(a, b uint64) (uint64, bool) {
	sum := a + b
	overflow := sum < a // If sum wrapped around, it will be less than a
	return sum, overflow
}
