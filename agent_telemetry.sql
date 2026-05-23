-- Create the telemetry table
CREATE TABLE IF NOT EXISTS public.agent_telemetry (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    metrics JSONB NOT NULL
);

-- Enable RLS (Optional but recommended)
ALTER TABLE public.agent_telemetry ENABLE ROW LEVEL SECURITY;

-- Create policy to allow inserts from authenticated or service role
CREATE POLICY "Allow inserts from anyone" ON public.agent_telemetry FOR INSERT WITH CHECK (true);
CREATE POLICY "Allow reads from anyone" ON public.agent_telemetry FOR SELECT USING (true);
