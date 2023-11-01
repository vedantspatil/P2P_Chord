module ChordNode
open System
open Akka
open Akka.Actor
open Akka.FSharp
open System.Collections.Generic
open System.Security.Cryptography
open System.Text
open PrintActor

let mutable m = 20

let StabilizeCycletimeMs = 100.0
let FixFingersCycletimeMs = 300.0

let mutable Node1 = 0
let mutable Node1_ref = null
let mutable Node2_ref = null
let chordSystem = ActorSystem.Create("ChordSystem")    
let printer = spawn chordSystem "PrinterActor" PrintActor  
let mutable hashSpace = pown 2 m

type FingerTableEntry(x:int, y:IActorRef) as this =
    let id = x
    let Ref = y
    member this.GetId() = x
    member this.GetRef() = y

type ChordCommands =
    | CreateNode of (int*IActorRef)
    | NotifyNode of (int*IActorRef)
    | StabilizeRing
    | FindNewNodeSuccessor of (int*IActorRef)
    | FoundNewNodeSuccessor of (int*IActorRef)
    | PredecessorRequest
    | PredecessorResponse of (int*IActorRef)
    | KeyLookupRequest of (int*int*int)
    | FixFingers
    | FindithSuccessor of (int*int*IActorRef)
    | FoundFingerEntry of (int*int*IActorRef)
    | StartLookups of (int)

let ChordNode (myId:int) (mailbox:Actor<_>) =    
    let mutable firstNode = 0
    let mutable mySuccessor = 0
    let mutable mySuccessorRef = null
    let mutable myPredecessor = 0
    let mutable myPredecessorRef = null
    let mutable myFingerTable = []
    let a = FingerTableEntry(0, null)
    let myFingerTable : FingerTableEntry[] = Array.create m a

    let rec iterate () = 
        actor {
            let! (message) = mailbox.Receive()
            let sender = mailbox.Sender()

            match message with 
            | CreateNode (otherId, otherRef) ->
                mySuccessor <- otherId
                myPredecessor <- otherId
                mySuccessorRef <- otherRef
                myPredecessorRef <- otherRef
                for i in 0..m-1 do
                    let tuple = FingerTableEntry(mySuccessor, mySuccessorRef)
                    myFingerTable.[i] <- tuple
                chordSystem.Scheduler.ScheduleTellRepeatedly(TimeSpan.FromSeconds(0.0),TimeSpan.FromMilliseconds(FixFingersCycletimeMs), mailbox.Self, FixFingers)
                chordSystem.Scheduler.ScheduleTellRepeatedly(TimeSpan.FromSeconds(0.0),TimeSpan.FromMilliseconds(StabilizeCycletimeMs), mailbox.Self, StabilizeRing)

            | NotifyNode(predecessorId, predecessorRef) ->
                myPredecessor <- predecessorId
                myPredecessorRef <- predecessorRef

            | FixFingers ->
                let mutable ithFinger = 0
                for i in 1..m-1 do
                    ithFinger <- ( myId + ( pown 2 i ) ) % int(hashSpace)
                    mailbox.Self <! FindithSuccessor(i, ithFinger, mailbox.Self)

            | FindithSuccessor(i, key, tellRef) ->
                if mySuccessor < myId && (key > myId || key < mySuccessor) then
                    tellRef <! FoundFingerEntry(i, mySuccessor, mySuccessorRef)
                elif key <= mySuccessor && key > myId then 
                    tellRef <! FoundFingerEntry(i, mySuccessor, mySuccessorRef)
                else 
                    let mutable Break = false
                    let mutable x = m
                    let mutable tempVal = key
                    if myId > key then 
                        tempVal <- key + hashSpace
                    while not Break do
                        x <- x - 1
                        if x < 0 then   
                            mySuccessorRef <! FindithSuccessor(i, key, tellRef)
                            Break <- true
                        else
                            let ithFinger = myFingerTable.[x].GetId()
                            if (ithFinger > myId && ithFinger <= tempVal) then 
                                let ithRef = myFingerTable.[x].GetRef()
                                ithRef <! FindithSuccessor(i, key, tellRef)
                                Break <- true                       
                    done                 

            | FoundFingerEntry(i, fingerId, fingerRef) ->
                let tuple = FingerTableEntry(fingerId, fingerRef)
                myFingerTable.[i] <- tuple

            | StabilizeRing ->
                if mySuccessor <> 0 then 
                    mySuccessorRef <! PredecessorRequest

            | PredecessorResponse(predecessorOfSuccessor, itsRef) ->                    
                if predecessorOfSuccessor <> myId then
                    mySuccessor <- predecessorOfSuccessor
                    mySuccessorRef <- itsRef
                // NotifyNode mysuccessor
                mySuccessorRef <! NotifyNode(myId, mailbox.Self)
                
            | PredecessorRequest->    
                sender <! PredecessorResponse(myPredecessor, myPredecessorRef)

            | FoundNewNodeSuccessor(isId, isRef) ->
                // Update successor information of self
                mySuccessor <- isId
                mySuccessorRef <- isRef
                // populate fingertable entry with successor - it will get corrected in next FixFingers call
                for i in 0..m-1 do
                    let tuple = FingerTableEntry(mySuccessor, mySuccessorRef)
                    myFingerTable.[i] <- tuple
                // start StabilizeRing scheduler
                chordSystem.Scheduler.ScheduleTellRepeatedly(TimeSpan.FromMilliseconds(0.0),TimeSpan.FromMilliseconds(StabilizeCycletimeMs), mailbox.Self, StabilizeRing)
                // start FixFingers scheduler
                chordSystem.Scheduler.ScheduleTellRepeatedly(TimeSpan.FromMilliseconds(0.0),TimeSpan.FromMilliseconds(FixFingersCycletimeMs), mailbox.Self, FixFingers)
                // NotifyNode Successor
                mySuccessorRef <! NotifyNode(myId, mailbox.Self)
         
            | KeyLookupRequest(key, hopCount, initiatedBy) ->
                if mySuccessor < myId && (key > myId || key <= mySuccessor) then
                    //printfn "\n iBy = %d key = %d at = %d hc = %d" initiatedBy key mySuccessor hopCount
                    printer <! FoundKey(hopCount)
                elif key <= mySuccessor && key > myId then 
                    //printfn "\n iby = %d key = %d at = %d hc = %d" initiatedBy key mySuccessor hopCount
                    printer <! FoundKey(hopCount)
                else
                    let mutable Break = false 
                    let mutable x = m
                    let mutable tempVal = key
                    if myId > key then 
                        tempVal <- key + hashSpace
                    while not Break do
                        x <- x - 1
                        if x < 0 then   
                            mySuccessorRef <! KeyLookupRequest(key, hopCount + 1, initiatedBy)
                            Break <- true
                        else
                            let ithFinger = myFingerTable.[x].GetId()
                            if (ithFinger > myId && ithFinger <= tempVal) then 
                                let ithRef = myFingerTable.[x].GetRef()
                                ithRef <! KeyLookupRequest(key, hopCount + 1, initiatedBy)
                                Break <- true                       
                    done 
                
            | StartLookups(numRequests) ->
                //printf "\n %d Starting lookups" myId
                let mutable tempKey = 0
                if mySuccessor <> Node1 then 
                    mySuccessorRef <! StartLookups(numRequests)
                for x in 1..numRequests do
                    tempKey <- Random().Next(1, int(hashSpace))
                    mailbox.Self <! KeyLookupRequest(tempKey, 1, myId)
                    //printfn "\n %d req key = %d" myId tempKey
                    System.Threading.Thread.Sleep(800)
            

            | FindNewNodeSuccessor(newId, seekerRef) ->
                if mySuccessor < myId && (newId > myId || newId < mySuccessor) then 
                    seekerRef <! FoundNewNodeSuccessor(mySuccessor, mySuccessorRef)
                    //printfn "\n %d (last node) Successor of %d is %d" myId newId mySuccessor
                elif newId <= mySuccessor && newId > myId then 
                    seekerRef <! FoundNewNodeSuccessor(mySuccessor, mySuccessorRef)
                    //printfn "\n %d Successor of %d is %d" myId newId mySuccessor
                else 
                    mySuccessorRef <! FindNewNodeSuccessor(newId, seekerRef)

            | _ -> ()

            return! iterate()
        }
    iterate()