from fastapi import FastAPI, HTTPException, Path
from .connect import get_db_connection,read_config
from pydantic import BaseModel

 
app = FastAPI()
 
# Pydantic model
class customerrr(BaseModel):
    customer_id: int
    name: str
    email:str
    phone: str
    address: str
 
 
@app.get("/")
def root():
    return {"message": "Welcome to the Customer API"}
 
 
# get all customers
@app.get("/customerrr")
def read_all_customer():
    config = read_config()
    conn = get_db_connection(config)
    if not conn:
        raise HTTPException(status_code = 500,detail="Datbase connection failed")
    cursor = conn.cursor()
    cursor.execute("SELECT customer_id,name,email,phone,address FROM customerrr")
    rows = cursor.fetchall()
    conn.close()
    return [dict(zip([column[0] for column in cursor.description],row)) for row in rows]
 


# get customers by ID
@app.get("/customerrr/{id}")
def read_customer(id: int = Path(..., description="The ID of the customer to retrieve")):
    config = read_config()
    conn = get_db_connection(config)
    if not conn:
        raise HTTPException(status_code = 500,detail="Datbase connection failed")
    cursor = conn.cursor()
    cursor.execute("SELECT customer_id,name,email,phone,address FROM customerrr WHERE customer_id = ?",(id,))
    row = cursor.fetchone()
    conn.close()
    if row:
        return dict(zip([column[0] for column in cursor.description],row))
    else:
        raise HTTPException(status_code=404,detail="Customer not found")
 


#create a new customer
@app.post("/customerrr/")
def create_customer(customerrr: customerrr):
    config = read_config()
    conn = get_db_connection(config)
    if not conn:
        raise HTTPException(status_code = 500,detail="Database connection failed")
    cursor = conn.cursor()
    cursor.execute(
        "INSERT INTO customerrr(customer_id,name,email,phone,address) VALUES(?,?,?,?,?)",
          (customerrr.customer_id,customerrr.name,customerrr.email,customerrr.phone,customerrr.address)
    )
    conn.commit()
    conn.close()
    return {"message" : "User created successfully"}
 


#update an existing customer based on customer_id
@app.put("/customerrr/{id}")
def update_customer(id:int,customerrr : customerrr):
    config = read_config()
    conn = get_db_connection(config)
    if not conn:
        raise HTTPException(status_code = 500,detail="Database connection failed")
    cursor = conn.cursor()
    cursor.execute(
        "UPDATE customerrr SET name = ?,email = ?,phone = ?,address = ? WHERE customer_id = ?",
        (customerrr.name,customerrr.email,customerrr.phone,customerrr.address,id)
    )
    conn.commit()
    conn.close()
    return {"message": "Customer updated successfully"}
 


#delete an existing customer based on customer id
@app.delete("/customerrr/{id}")
def delete_customer(id:int):
    config = read_config()
    conn = get_db_connection(config)
    if not conn:
        raise HTTPException(status_code = 500,detail="Database connection failed")
    cursor = conn.cursor()
    cursor.execute("DELETE FROM customerrr WHERE customer_id = ?",(id,))
    conn.commit()
    conn.close()
    return {"message" : "Customer deleted successfully"}
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app,host = "127.0.0.1",port=8001)