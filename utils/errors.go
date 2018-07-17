package utils

import (
    "context";
)

func WaitUntilAllSuccessful(ctx context.Context, n int, errors chan error) error {
    nDone := 0
    for {
        select {
        case <-ctx.Done():
            return nil
        case err := <-errors:
            if err == nil {
                nDone++
            } else {
                return err
            }
            if nDone == n {
                return nil
            }
        }
    }
    return nil
}
