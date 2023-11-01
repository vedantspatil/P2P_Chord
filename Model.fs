module Model
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