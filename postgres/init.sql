CREATE TABLE public.customers (
    id    SERIAL PRIMARY KEY,
    name  VARCHAR(100) NOT NULL,
    email VARCHAR(100) NOT NULL,
    city  VARCHAR(100)
);
ALTER TABLE public.customers REPLICA IDENTITY FULL;

INSERT INTO public.customers (name, email, city) VALUES
 ('Sokha',  'sokha@example.com',  'Phnom Penh'),
 ('Dara',   'dara@example.com',   'Siem Reap');
