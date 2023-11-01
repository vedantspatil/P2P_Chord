module Model
open Akka.Actor
open Akka.FSharp
open Akka.Configuration
open System.Security.Cryptography
open System.Text

let mutable numNodes = 0
let mutable numRequests = 0
let mutable m = 20
let mutable Node1 = 0

let StabilizeCycletimeMs = 100.0
let FixFingersCycletimeMs = 300.0

let mutable hashSpace = pown 2 m

let configuration = 
    ConfigurationFactory.ParseString(
        @"akka {            
            stdout-loglevel : ERROR
            loglevel : ERROR
            log-dead-letters = 0
            log-dead-letters-during-shutdown = off
        }")

let chordSystem = ActorSystem.Create("ChordSystem")

type ChordCommands =
    | ChordStart of (int*int)
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