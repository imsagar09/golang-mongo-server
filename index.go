package main
//necessary imports
import (
	"net/http"
	"fmt"
	"encoding/json"
	"context"
	"time"
	"log"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"	
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)
//error class
type Error struct{
	StatusCode int		`json:"status_code"`
	ErrorMessage string	`json:"error_message"`
}
//new meeting
type new_meet struct{
	Meet_ID string  `json:"Id"`
}
//participant shema
type participant struct{
	Name string `json:"Name" bson:"name"`
	Email string `json:"Email" bson:"email"`
	RSVP string `json:"RSVP" bson:"rsvp"`
}
//meeting schema
type meeting struct{
	Id primitive.ObjectID `bson:"_id"`
	Title string `json:"Title" bson:"title"`
	Part []participant `json:"Participants" bson:"participants" `
	Start   time.Time `json:"Start Time" bson:"start" `
	End time.Time `json:"End Time" bson:"end"`
	Stamp time.Time `bson:"stamp"`
}
//schema for results of conditional meetings
type conditional_meets struct{
	Meetings []meeting  `json:"meetings"`
}
//invalid request response writer function
func invalid_request(w http.ResponseWriter, statCode int, message string){
	w.Header().Set("Content-Type", "application/json")
	switch statCode {
	case 400: w.WriteHeader(http.StatusBadRequest)
	case 403: w.WriteHeader(http.StatusForbidden)
	case 404: w.WriteHeader(http.StatusNotFound)
	default: w.WriteHeader(http.StatusNotFound)
	}
	err := Error {
							StatusCode: statCode,
							ErrorMessage: message}
	json.NewEncoder(w).Encode(err)
}

//helper function to coneect to DB
func connectdb(ctx context.Context) (*mongo.Collection){
	client, err := mongo.NewClient(options.Client().ApplyURI("mongodb://localhost:27017"))
	if err != nil {
		log.Fatal(err)
	}
	
	err = client.Connect(ctx)
	if err != nil {
		log.Fatal(err)
	}

	appointyDatabase := client.Database("appointy-task-ritvix")
    meetingCollection := appointyDatabase.Collection("meetings")

	//returns collection object
	return meetingCollection
}

func main(){
	fmt.Println("Server is up")
	http.HandleFunc("/meetings" , meets_handler) // handler for /meetings end point
	http.HandleFunc("/meeting/" , meeting_handler) // handler for rooted /meeting/
	fmt.Println(http.ListenAndServe(":8082", nil)); // listen to port 8082
}

//handle requests at /meetings
func meets_handler(w http.ResponseWriter, r *http.Request){
	switch r.Method{
	//if method is POST
	case "POST":
		//disallow query strings with POST method
		if keys := r.URL.Query(); len(keys)!=0{
			invalid_request(w, 400, "Queries not allowed at this endpoint with this method")
		}else{
		//error handling if request not JSON
		if ua := r.Header.Get("Content-Type"); ua!="application/json"{
			invalid_request(w, 400, "This end point accepts only JSON request body")
		}else{
			var m meeting
			dec := json.NewDecoder(r.Body)
			dec.DisallowUnknownFields()
			err := dec.Decode(&m)
			//error if meeting details are not in right format
			if err != nil {
				invalid_request(w, 400, "Please recheck the meeting information")
				return 
			}
			m.Stamp = time.Now()  //assign Creation stamp
			m.Id = primitive.NewObjectID() //assign unique ObjectID
			ctx, _ := context.WithTimeout(context.Background(), 10*time.Second) //timeout
			meetingCollection := connectdb(ctx)  //meeting collection
			//check for overlap of participants
			final_check := false
			//iterate over al participants and find clashes is db
			for _, particip := range m.Part{
				
				var check meeting
				check1  := true
				check2  := true
				check3  := true 
				if err = meetingCollection.FindOne(ctx, bson.M{"start": bson.D{{"$lte", m.Start}}, "end": bson.D{{"$gt", m.Start}}, "participants.email": particip.Email}).Decode(&check); err!=nil{
					check1 = false
				}
				if err = meetingCollection.FindOne(ctx, bson.M{"start": bson.D{{"$lt", m.End}}, "end": bson.D{{"$gte",m.End}}, "participants.email":particip.Email}).Decode(&check); err!=nil{
					check2 = false
				}
				if err = meetingCollection.FindOne(ctx, bson.M{"start": bson.D{{"$gte", m.Start}}, "end": bson.D{{"$lte", m.End}}, "participants.email":particip.Email}).Decode(&check); err!=nil{
					check3 = false
				}
				if check1 || check2 || check3 {
					final_check =true
				}
			
			}
			if final_check{
				invalid_request(w, 400, "Meeting clashes with other meeting/s with some common participant/s")

			}else{
					insertResult, err := meetingCollection.InsertOne(ctx, m)
					if err != nil {
						log.Fatal(err)
						return
					}

					//write back meeting id as JSON response
					w.Header().Set("Content-Type", "application/json")
					meet := new_meet{
							Meet_ID: insertResult.InsertedID.(primitive.ObjectID).Hex()}
					json.NewEncoder(w).Encode(meet)
			}

		}
	}
	//if method is GET
	case "GET":
		keys := r.URL.Query()
		//cases to allow only valid queries
		switch len(keys){
		//no query string error
		case 0:invalid_request(w, 400, "Not a valid query at this end point")
		case 1:
			//extract participant email
			if email, ok := keys["participant"]; !ok || len(email[0])<1{
				invalid_request(w, 400, "Not a valid query at this end point")
			}else {
				var meets []meeting
				ctx, _ := context.WithTimeout(context.Background(), 10*time.Second) //timeout
				meetingCollection := connectdb(ctx) //collection meetings
				if len(email)>1{
					invalid_request(w, 400, "Only one participant can be queried at a time")
					return
				}
				//query the collection for the mail id
				cursor, err := meetingCollection.Find(ctx, bson.M{"participants.email":bson.M{"$eq":email[0]}})
				if err != nil {
					log.Fatal(err)
					return
				}
				if err = cursor.All(ctx, &meets); err != nil {
					log.Fatal(err)
					return
				}
				//write back all his/her meetings as an array
				my_meets := conditional_meets{
					Meetings: meets}
				w.Header().Set("Content-Type", "application/json")
				json.NewEncoder(w).Encode(my_meets)
			}
		case 2:
			start, okStart := keys["start"]
			end, okEnd := keys["end"]
			//check both start and end time are provided, else error
			if !okStart || !okEnd {invalid_request(w, 400, "Not a valid query at this end point")
			}else{
				start_time := start[0]
				end_time := end[0]
				fmt.Println(start_time, end_time)
				start_tim, err := time.Parse(time.RFC3339, start_time)
				//check if the time format is valid
				if err!=nil{
					invalid_request(w, 400, "Please enter date and time in RFC3339 format- YY-MM-DDTHH-MM-SSZ")
					return 
				}
				end_tim, err := time.Parse(time.RFC3339, end_time)
				if err!=nil{
					invalid_request(w, 400, "Please enter date and time in RFC3339 format - YY-MM-DDTHH-MM-SSZ")
					return 
				}

				var meets []meeting
				ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
				
				meetingCollection := connectdb(ctx)
				
				//query the DB for the time window
				cursor, err := meetingCollection.Find(ctx, bson.M{"start": bson.D{{"$gt", start_tim}}, "end": bson.D{{"$lt", end_tim}}})
				if err != nil {
					log.Fatal(err)
					return
				}
				if err = cursor.All(ctx, &meets); err != nil {
					log.Fatal(err)
					return
				}
				//return all such meetings as array
				my_meets := conditional_meets{
					Meetings: meets}
				w.Header().Set("Content-Type", "application/json")
				json.NewEncoder(w).Encode(my_meets)
			}

		default:invalid_request(w, 400, "Not a valid query at this end point")
		}
	//disallow any other method
	default:invalid_request(w, 403, "Not a valid method at this end point")
	}
}
//handler for meeting/ root
func meeting_handler(w http.ResponseWriter, r *http.Request){
	switch r.Method{
	case "GET":
		//extract meeting id from url
		if meet_id := r.URL.Path[len("/meeting/"):]; len(meet_id)==0{
			invalid_request(w, 400, "Not a valid Meeting ID")
		}else{
			//check forvalid id
			id, err := primitive.ObjectIDFromHex(meet_id)
			if err!=nil{
				invalid_request(w, 400, "Not a valid Meeting ID")
				return
			}

			var meet meeting
			filter := bson.M{"_id": id}
			ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
			meetingCollection := connectdb(ctx)
			err = meetingCollection.FindOne(ctx, filter).Decode(&meet)
			if err != nil {
				invalid_request(w, 404, "No meeting found with this ID")
				return
			}
			//write back the meeting info
			w.Header().Set("Content-Type", "application/json")
			// fmt.Println(meet)
			json.NewEncoder(w).Encode(meet)
		}
	default:invalid_request(w, 403, "Not a valid method at this end point")
	}
}
