open System
open Akka.Actor
open Akka.FSharp
open System.Collections.Generic
open Chord
open Model

let mutable ActorRef: IActorRef = null
let mutable Node1_ref = null
let mutable Node2_ref = null

let Actor (mailbox:Actor<_>) =    
    let mutable Node2 = 0
    let mutable tempNode = 0
    let mutable tempNodeRef = null
    let list = new List<int>()

    let rec iterate () = 
        actor {
            let! (message) = mailbox.Receive()

            match message with 
            | ChordStart(numNodes, numRequests) ->
                Node1 <- Random().Next(int(hashSpace))
                printfn "\n\n Added node 1: ID: %d" Node1
                Node1_ref <- spawn chordSystem (sprintf "%d" Node1) (ChordNode Node1)
                Node2 <- Random().Next(int(hashSpace))
                printfn "\n\n Added node 2: ID: %d" Node2
                Node2_ref <- spawn chordSystem (sprintf "%d" Node2) (ChordNode Node2)

                Node1_ref <! CreateNode(Node2, Node2_ref)
                Node2_ref <! CreateNode(Node1, Node1_ref)

                for x in 3..numNodes do
                    System.Threading.Thread.Sleep(300)
                    tempNode <- [ 1 .. hashSpace ]
                        |> List.filter (fun x -> (not (list.Contains(x))))
                        |> fun y -> y.[Random().Next(y.Length - 1)]
                    list.Add(tempNode)
                    printfn "\n\n Added node %d: ID: %d" x tempNode
                    tempNodeRef <- spawn chordSystem (sprintf "%d" tempNode) (ChordNode tempNode)
                    Node1_ref <! FindNewNodeSuccessor(tempNode, tempNodeRef)  
                
                printfn "\n Ring stabilized"
                System.Threading.Thread.Sleep(8000)
                Node1_ref <! StartLookups(numRequests)

            | _ -> ()

            return! iterate()
        }
    iterate()


[<EntryPoint>]
let main argv =
    numNodes <-  argv.[0] |> int
    numRequests <- argv.[1] |> int

    ActorRef <- spawn chordSystem "MainActor" Actor
    ActorRef <! ChordStart(numNodes, numRequests)

    chordSystem.WhenTerminated.Wait()
        
    0
