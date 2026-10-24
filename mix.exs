defmodule Jobforge.MixProject do
  use Mix.Project

  @version "0.1.0"

  def project do
    [
      app: :jobforge,
      version: @version,
      elixir: "~> 1.15",
      start_permanent: Mix.env() == :prod,
      deps: [],
      description: "A small, dependency-free background job queue for Elixir built on OTP.",
      source_url: "https://github.com/avohra25/jobforge"
    ]
  end

  def application do
    [extra_applications: [:logger, :crypto]]
  end
end
