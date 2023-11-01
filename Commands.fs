module Commands
open Akka.Actor

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
    
