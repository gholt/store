package main

import (
"os"
"fmt"
)

func main() {
    if len(os.Args) == 0 {
        fmt.Println("old start/stop")
        os.Exit(1)
    } else {
        switch os.Args[1] {
            case "old":
                old()
            case "start/stop":
            startstop()
            default:
                fmt.Printf("unknown command %#v\n", old)
        os.Exit(1)
        }
    }
}
