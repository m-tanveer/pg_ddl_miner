CREATE OR REPLACE FUNCTION public.updated_at()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
        BEGIN    NEW.updated_at = now();    RETURN NEW;
        END;
      $function$
