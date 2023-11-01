
module PrintActor
open System
open Akka
open Akka.Actor
open Akka.FSharp
open System.Collections.Generic
open System.Security.Cryptography
open System.Text

let mutable numNodes = 0
let mutable numRequests = 0

type ChordCommands =
    | FoundKey of (int)

let PrintActor (mailbox: Actor<_>) =
    let mutable totalHops = 0
    let mutable requestsCount = 0
    let endCondition = numNodes * numRequests

    let rec iterate () =
        actor {
            let! (message) = mailbox.Receive()
            let sender = mailbox.Sender()

            match message with 
            | FoundKey (hopCount) ->
                totalHops <- totalHops + hopCount
                requestsCount <- requestsCount + 1
                printfn "\n Key: %d Hops: %d" requestsCount hopCount

                if requestsCount = endCondition then 
                    let avgHopCount = float(totalHops)/float(requestsCount)
                    printfn "\nAverage Hopcount = %.2f" avgHopCount
                    mailbox.Context.System.Terminate() |> ignore
            | _ -> ()
            return! iterate()
        }

    iterate ()