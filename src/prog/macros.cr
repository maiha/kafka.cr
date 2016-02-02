macro val(s)
  def {{s.target}}
    @{{s.target}} ||= {{s.value}}
  end
end

macro var(name, default)
  def {{name.var.id}}
    @{{name.var.id}} || {{default}}
  end

  def {{name.var.id}}=(@{{name.var.id}} : {{name.type}})
  end
end
