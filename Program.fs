open System
open Akka
open Akka.Actor
open Akka.FSharp
open Akka.Configuration
open System.Collections.Generic
open System.Security.Cryptography
open System.Text

let mutable ActorRef = null

let mutable numNodes = 0
let mutable numRequests = 0
let mutable m = 20
let mutable Node1 = 0
let mutable Node1_ref = null
let mutable Node2_ref = null
let StabilizeCycletimeMs = 100.0
let FixFingersCycletimeMs = 300.0

let mutable hashSpace = pown 2 m
let chordSystem = ActorSystem.Create("ChordSystem")

type ChordCommands =
    | ChordStart of (int*int)
    | CreateNode of (int*IActorRef)
    | NotifyNext of (int*IActorRef)
    | Stabilize
    | FindNewNodeForRing of (int*IActorRef)
    | FoundNewNodeForRing of (int*IActorRef)
    | PrevNodeRequest
    | PrevNodeResponse of (int*IActorRef)
    | KeyLookup of (int*int*int)
    | FixFingers
    | SearchithSuccessor of (int*int*IActorRef)
    | FingerEntryFound of (int*int*IActorRef)
    | StartLookups of (int)
    | FoundKey of (int)
    


let updateElement index element list = 
  list |> List.mapi (fun i v -> if i = index then element else v)

type FingerTableEntry(x:int, y:IActorRef) as this =
    let id = x
    let Ref = y
    member this.GetId() = x
    member this.GetRef() = y

let GetHash data =
    use sha1Hash = SHA1Managed.Create()
    let mutable hash = sha1Hash.ComputeHash(Encoding.UTF8.GetBytes(data:string):byte[]) |> bigint
    if hash.Sign = -1 then
        hash <- bigint.Negate(hash)
    hash

let hashfunc (m:double) (nodeNum:int) = 
    let name = string(nodeNum)
    let hash = GetHash name 
    let x = m |> bigint
    let hashKey = (hash) % x  
    let nodeName: string = hashKey |> string
    nodeName

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


let printer = spawn chordSystem "PrinterActor" PrintActor  


let ChordFunc (myId:int) (mailbox:Actor<_>) =    
    let mutable firstN = 0
    let mutable nextNode = 0
    let mutable nextNodeRef = null
    let mutable prevNode = 0
    let mutable prevNodeRef = null
    let mutable FingerTable = []
    let a = FingerTableEntry(0, null)
    let FingerTable : FingerTableEntry[] = Array.create m a

    let rec iterate () = 
        actor {
            let! (message) = mailbox.Receive()
            let sender = mailbox.Sender()

            match message with 
            | CreateNode (newId, newRef) ->
                nextNode <- newId
                prevNode <- newId
                nextNodeRef <- newRef
                prevNodeRef <- newRef
                for i in 0..m-1 do
                    let tuple = FingerTableEntry(nextNode, nextNodeRef)
                    FingerTable.[i] <- tuple
                chordSystem.Scheduler.ScheduleTellRepeatedly(TimeSpan.FromSeconds(0.0),TimeSpan.FromMilliseconds(FixFingersCycletimeMs), mailbox.Self, FixFingers)
                chordSystem.Scheduler.ScheduleTellRepeatedly(TimeSpan.FromSeconds(0.0),TimeSpan.FromMilliseconds(StabilizeCycletimeMs), mailbox.Self, Stabilize)

             // |Populate successor list
                //for i = 0 to successorListLength-1 do
                //successorList.Add(nextNode)

            | FingerEntryFound(i, fingerId, fingerRef) ->
                let tuple = FingerTableEntry(fingerId, fingerRef)
                FingerTable.[i] <- tuple

            | SearchithSuccessor(i, key, tRef) ->
                if key <= nextNode && key > myId then 
                    tRef <! FingerEntryFound(i, nextNode, nextNodeRef)
                elif nextNode < myId && (key > myId || key < nextNode) then
                    tRef <! FingerEntryFound(i, nextNode, nextNodeRef)
                else 
                    let mutable Break = false
                    let mutable x = m
                    let mutable temp = key
                    if myId > key then 
                        temp <- key + hashSpace
                    while not Break do
                        x <- x - 1
                        if x < 0 then   
                            nextNodeRef <! SearchithSuccessor(i, key, tRef)
                            Break <- true
                        else
                            let iPosFinger = FingerTable.[x].GetId()
                            if (iPosFinger > myId && iPosFinger <= temp) then 
                                let ithRef = FingerTable.[x].GetRef()
                                ithRef <! SearchithSuccessor(i, key, tRef)
                                Break <- true                       
                    done   
            
            | FixFingers ->
                let mutable iPosFinger = 0
                for i in 1..m-1 do
                    iPosFinger <- ( myId + ( pown 2 i ) ) % int(hashSpace)
                    mailbox.Self <! SearchithSuccessor(i, iPosFinger, mailbox.Self)
            
            | NotifyNext(predecessorId, predecessorRef) ->
                prevNode <- predecessorId
                prevNodeRef <- predecessorRef
              
            | PrevNodeResponse(predecessorOfNext, itsRef) ->                    
                if predecessorOfNext <> myId then
                    nextNode <- predecessorOfNext
                    nextNodeRef <- itsRef
                // Notify the nextNode
                nextNodeRef <! NotifyNext(myId, mailbox.Self)

            | PrevNodeRequest->    
                sender <! PrevNodeResponse(prevNode, prevNodeRef)              

            | Stabilize ->

                //if !nextNodeRef.IsNobody() then

                 // Remove failed successor
                //successorList.Remove(nextNode)

                 // Update successor 
                //nextNode <- successorList[0]
                if nextNode <> 0 then 
                    nextNodeRef <! PrevNodeRequest

            | FindNewNodeForRing(newId, seekRef) ->
                if nextNode < myId && (newId > myId || newId < nextNode) then 
                    seekRef <! FoundNewNodeForRing(nextNode, nextNodeRef)
                    //printfn "\n %d (last node) Successor of %d is %d" myId newId nextNode
                elif newId <= nextNode && newId > myId then 
                    seekRef <! FoundNewNodeForRing(nextNode, nextNodeRef)
                    //printfn "\n %d Successor of %d is %d" myId newId nextNode
                else 
                    nextNodeRef <! FindNewNodeForRing(newId, seekRef)
           

            | FoundNewNodeForRing(isId, isRef) ->
                // Update successor information of self
                nextNode <- isId
                nextNodeRef <- isRef
                // populate fingertable entry with successor - it will get corrected in next FixFingers call
                for i in 0..m-1 do
                    let tuple = FingerTableEntry(nextNode, nextNodeRef)
                    FingerTable.[i] <- tuple
                // start Stabilize scheduler
                chordSystem.Scheduler.ScheduleTellRepeatedly(TimeSpan.FromMilliseconds(0.0),TimeSpan.FromMilliseconds(StabilizeCycletimeMs), mailbox.Self, Stabilize)
                // start FixFingers scheduler
                chordSystem.Scheduler.ScheduleTellRepeatedly(TimeSpan.FromMilliseconds(0.0),TimeSpan.FromMilliseconds(FixFingersCycletimeMs), mailbox.Self, FixFingers)
                // NotifyNext Successor
                nextNodeRef <! NotifyNext(myId, mailbox.Self)
         
            | KeyLookup(key, hopCount, initiatedBy) ->
                //if successorList.Length > 0 then
                //	let next = closestPreceding(key, successorList)
                //	next <! KeyLookup(key, hopCount+1, initiatedBy)
                if nextNode < myId && (key > myId || key <= nextNode) then
                    //printfn "\n iBy = %d key = %d at = %d hc = %d" initiatedBy key nextNode hopCount
                    printer <! FoundKey(hopCount)
                elif key <= nextNode && key > myId then 
                    //printfn "\n iby = %d key = %d at = %d hc = %d" initiatedBy key nextNode hopCount
                    printer <! FoundKey(hopCount)
                else
                    let mutable Break = false 
                    let mutable x = m
                    let mutable temp = key
                    if myId > key then 
                        temp <- key + hashSpace
                    while not Break do
                        x <- x - 1
                        if x < 0 then   
                            nextNodeRef <! KeyLookup(key, hopCount + 1, initiatedBy)
                            Break <- true
                        else
                            let iPosFinger = FingerTable.[x].GetId()
                            if (iPosFinger > myId && iPosFinger <= temp) then 
                                let ithRef = FingerTable.[x].GetRef()
                                ithRef <! KeyLookup(key, hopCount + 1, initiatedBy)
                                Break <- true                       
                    done 
                
            | StartLookups(numRequests) ->
                //printf "\n %d Starting lookups" myId
                let mutable tempKey = 0
                if nextNode <> Node1 then 
                    nextNodeRef <! StartLookups(numRequests)
                for x in 1..numRequests do
                    tempKey <- Random().Next(1, int(hashSpace))
                    mailbox.Self <! KeyLookup(tempKey, 1, myId)
                    //printfn "\n %d req key = %d" myId tempKey
                    System.Threading.Thread.Sleep(800)
            | _ -> ()

            return! iterate()
        }
    iterate()

let Actor (mailbox:Actor<_>) =    
    let mutable Node2 = 0
    let mutable tempNode = 0
    let mutable tempNodeName = ""
    let mutable tempNodeRef = null
    let mutable tempKey = 0
    let list = new List<int>()

    let rec iterate () = 
        actor {
            let! (message) = mailbox.Receive()

            match message with 
            | ChordStart(numNodes, numRequests) ->
                Node1 <- Random().Next(int(hashSpace))
                printfn "\n\n Added node 1: ID: %d" Node1
                Node1_ref <- spawn chordSystem (sprintf "%d" Node1) (ChordFunc Node1)
                Node2 <- Random().Next(int(hashSpace))
                printfn "\n\n Added node 2: ID: %d" Node2
                Node2_ref <- spawn chordSystem (sprintf "%d" Node2) (ChordFunc Node2)

                Node1_ref <! CreateNode(Node2, Node2_ref)
                Node2_ref <! CreateNode(Node1, Node1_ref)

                for x in 3..numNodes do
                    System.Threading.Thread.Sleep(300)
                    tempNode <- [ 1 .. hashSpace ]
                        |> List.filter (fun x -> (not (list.Contains(x))))
                        |> fun y -> y.[Random().Next(y.Length - 1)]
                    list.Add(tempNode)
                    printfn "\n\n Added node %d: ID: %d" x tempNode
                    tempNodeRef <- spawn chordSystem (sprintf "%d" tempNode) (ChordFunc tempNode)
                    Node1_ref <! FindNewNodeForRing(tempNode, tempNodeRef)  
                
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

