package framework

import "fmt"

// SetState saves a specific state for the current user in the Yowza! database.
func (c *Context) SetState(state string) error {
	if c.Update.UserID == "" {
		return fmt.Errorf("user_id is empty, cannot set state")
	}

	key := "state:" + c.Update.UserID
	return c.DB.Put(key, []byte(state))
}

// GetState retrieves the current state of the user. Returns empty string if none.
func (c *Context) GetState() (string, error) {
	if c.Update.UserID == "" {
		return "", fmt.Errorf("user_id is empty, cannot get state")
	}

	key := "state:" + c.Update.UserID
	val, err := c.DB.Get(key)
	if err != nil {
		return "", err
	}

	if val == nil {
		return "", nil // No state set
	}

	return string(val), nil
}

// ClearState removes the state for the current user.
func (c *Context) ClearState() error {
	if c.Update.UserID == "" {
		return fmt.Errorf("user_id is empty, cannot clear state")
	}

	key := "state:" + c.Update.UserID
	return c.DB.Delete(key)
}
